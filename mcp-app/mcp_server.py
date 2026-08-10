"""
Stock Research Assistant MCP server.

Exposes read AND write tools over MCP so a Databricks Agent Bricks agent
can both retrieve context and take real actions against Lakebase:
    - get_price_history(symbol, days)
    - compare_tickers(symbols)
    - search_context(query, symbol, limit)
    - get_recent_news(symbol, limit)
    - flag_notable_changes(since_days)
    - add_to_watchlist(symbol)
    - remove_from_watchlist(symbol)
    - save_research_note(symbol, note_text)
    - save_analysis_report(symbols, report_text, report_type)

Mirrors alpaca_mcp_server.py's structure: FastMCP, thin @mcp.tool
wrappers, end-user identity resolved from the X-Forwarded-Email header
(Databricks Apps inject this), falling back to the service principal for
local development.

Run locally:
    python mcp_server.py
"""

import logging
import os
from contextvars import ContextVar

from fastmcp import FastMCP
from sentence_transformers import SentenceTransformer
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

import lakebase
import stocks_broker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stock-research-mcp-server")

EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

_embedding_model = None


def _get_embedding_model():
    global _embedding_model
    if _embedding_model is None:
        logger.info(f"Loading embedding model: {EMBEDDING_MODEL_NAME}")
        _embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    return _embedding_model


_request_context: ContextVar[dict] = ContextVar("request_context", default={})


def _get_end_user_email() -> str:
    headers = _request_context.get()
    forwarded_email = headers.get("x-forwarded-email")
    if forwarded_email:
        return forwarded_email

    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    return w.current_user.me().user_name or "local-dev@example.com"


def _get_or_create_user(email: str) -> dict:
    rows = lakebase.run_query("SELECT user_id, email FROM users WHERE email = %s", (email,))
    if rows:
        return rows[0]
    rows = lakebase.run_write_returning(
        "INSERT INTO users (email) VALUES (%s) RETURNING user_id, email", (email,)
    )
    return rows[0]


def _get_or_create_watchlist(user_id: int) -> int:
    rows = lakebase.run_query(
        "SELECT watchlist_id FROM watchlists WHERE user_id = %s", (user_id,)
    )
    if rows:
        return rows[0]["watchlist_id"]
    rows = lakebase.run_write_returning(
        "INSERT INTO watchlists (user_id) VALUES (%s) RETURNING watchlist_id", (user_id,)
    )
    return rows[0]["watchlist_id"]


def _ensure_company(symbol: str) -> None:
    exists = lakebase.run_query("SELECT 1 FROM companies WHERE symbol = %s", (symbol,))
    if exists:
        return
    profile = stocks_broker.get_company_profile(symbol)
    lakebase.run_write(
        """
        INSERT INTO companies (symbol, name, sector, description, updated_at)
        VALUES (%s, %s, %s, %s, now())
        ON CONFLICT (symbol) DO NOTHING
        """,
        (symbol, profile.get("name"), profile.get("sic_description"), profile.get("description")),
    )


mcp = FastMCP("stock-research-assistant")


class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        _request_context.set({
            "x-forwarded-email": request.headers.get("x-forwarded-email"),
        })
        return await call_next(request)


# ============================================================
# Read tools
# ============================================================

@mcp.tool
def get_price_history(symbol: str, days: int = 30) -> dict:
    """
    Get recent price history for a ticker and summarize its performance.

    Args:
        symbol: Stock ticker symbol, e.g. "AAPL".
        days: Number of most recent days of price history to return (default 30).

    Returns:
        A dict with symbol, a list of daily snapshots (price, daily_return_pct,
        moving_avg_7d, snapshot_time), and a summary (latest_price,
        period_return_pct - the cumulative % change over the window).
        On failure, returns {"status": "error", "message": "..."}.
    """
    try:
        symbol = symbol.strip().upper()
        rows = lakebase.run_query(
            """
            SELECT price, daily_return_pct, moving_avg_7d, snapshot_time
            FROM price_snapshots WHERE symbol = %s
            ORDER BY snapshot_time DESC LIMIT %s
            """,
            (symbol, days),
        )
        if not rows:
            return {"status": "error", "message": f"No price history found for {symbol}."}

        latest = rows[0]
        oldest = rows[-1]
        period_return_pct = None
        if oldest["price"] and float(oldest["price"]) != 0:
            period_return_pct = round(
                ((float(latest["price"]) - float(oldest["price"])) / float(oldest["price"])) * 100, 2
            )

        return {
            "symbol": symbol,
            "history": rows,
            "summary": {
                "latest_price": latest["price"],
                "period_return_pct": period_return_pct,
                "days_covered": len(rows),
            },
        }
    except Exception as e:
        logger.exception(f"Failed to get price history for {symbol!r}")
        return {"status": "error", "message": str(e)}


@mcp.tool
def compare_tickers(symbols: list[str]) -> dict:
    """
    Compare multiple tickers on recent price action.

    Args:
        symbols: List of ticker symbols to compare, e.g. ["AAPL", "MSFT"].

    Returns:
        A dict mapping each symbol to its latest price, daily_return_pct,
        and moving_avg_7d, plus a `ranked_by_daily_return` list ordering
        symbols from best to worst 1-day performer.
        On failure, returns {"status": "error", "message": "..."}.
    """
    try:
        comparison = {}
        for symbol in symbols:
            symbol = symbol.strip().upper()
            rows = lakebase.run_query(
                """
                SELECT price, daily_return_pct, moving_avg_7d, snapshot_time
                FROM price_snapshots WHERE symbol = %s
                ORDER BY snapshot_time DESC LIMIT 1
                """,
                (symbol,),
            )
            comparison[symbol] = rows[0] if rows else None

        ranked = sorted(
            [s for s in comparison if comparison[s] and comparison[s]["daily_return_pct"] is not None],
            key=lambda s: comparison[s]["daily_return_pct"],
            reverse=True,
        )

        return {"comparison": comparison, "ranked_by_daily_return": ranked}
    except Exception as e:
        logger.exception(f"Failed to compare tickers {symbols!r}")
        return {"status": "error", "message": str(e)}


@mcp.tool
def search_context(query: str, symbol: str | None = None, limit: int = 5) -> dict:
    """
    Semantic search over embedded company profiles, news, filings, and
    earnings summaries - e.g. "companies exposed to rising interest rates
    in the regional banking sector" rather than a plain keyword lookup.

    Args:
        query: Natural language search query.
        symbol: Optional ticker to restrict results to (via source_id match
            for company_profile, or joining news_articles/company_documents
            for the other source types).
        limit: Max number of chunks to return (default 5).

    Returns:
        A dict with query and a list of matches, each with source_type,
        source_id, chunk_text, and similarity.
        On failure, returns {"status": "error", "message": "..."}.
    """
    try:
        if not query or not query.strip():
            return {"status": "error", "message": "query is required"}

        model = _get_embedding_model()
        vector = model.encode([query])[0]
        vector_str = "[" + ",".join(str(float(x)) for x in vector) + "]"

        if symbol:
            symbol = symbol.strip().upper()
            rows = lakebase.run_query(
                """
                SELECT source_type, source_id, chunk_text,
                       1 - (embedding <=> %s::vector) AS similarity
                FROM document_embeddings
                WHERE source_id = %s
                   OR source_id IN (SELECT id FROM news_articles WHERE symbol = %s)
                   OR source_id IN (SELECT id FROM company_documents WHERE symbol = %s)
                ORDER BY embedding <=> %s::vector
                LIMIT %s
                """,
                (vector_str, symbol, symbol, symbol, vector_str, limit),
            )
        else:
            rows = lakebase.run_query(
                """
                SELECT source_type, source_id, chunk_text,
                       1 - (embedding <=> %s::vector) AS similarity
                FROM document_embeddings
                ORDER BY embedding <=> %s::vector
                LIMIT %s
                """,
                (vector_str, vector_str, limit),
            )

        return {"query": query, "results": rows}
    except Exception as e:
        logger.exception(f"Search failed for query {query!r}")
        return {"status": "error", "message": str(e)}


@mcp.tool
def get_recent_news(symbol: str, limit: int = 10) -> dict:
    """
    Get recent news articles already synced for a ticker.

    Args:
        symbol: Stock ticker symbol, e.g. "AAPL".
        limit: Max number of articles to return (default 10).

    Returns:
        A dict with symbol and a list of articles (title, description,
        source, published_at, url).
        On failure, returns {"status": "error", "message": "..."}.
    """
    try:
        symbol = symbol.strip().upper()
        rows = lakebase.run_query(
            """
            SELECT id, title, description, source, published_at, url
            FROM news_articles WHERE symbol = %s
            ORDER BY published_at DESC LIMIT %s
            """,
            (symbol, limit),
        )
        return {"symbol": symbol, "articles": rows}
    except Exception as e:
        logger.exception(f"Failed to get news for {symbol!r}")
        return {"status": "error", "message": str(e)}


@mcp.tool
def flag_notable_changes(since_days: int = 1) -> dict:
    """
    Flag notable price moves or news for the current user's watchlist
    within the last `since_days` days.

    "Notable" is defined as: |daily_return_pct| > 5% on the most recent
    snapshot, OR at least one news article published within the window.
    (Simplification note: this checks a fixed lookback window, not a
    true "since your last visit" timestamp, since no last-seen tracking
    exists yet.)

    Args:
        since_days: How many days back to consider "recent" (default 1).

    Returns:
        A dict with a list of flagged tickers, each with symbol, reason
        ("price_move" or "news"), and supporting detail.
        On failure, returns {"status": "error", "message": "..."}.
    """
    try:
        email = _get_end_user_email()
        user = _get_or_create_user(email)
        watchlist_id = _get_or_create_watchlist(user["user_id"])

        symbols_rows = lakebase.run_query(
            "SELECT symbol FROM watchlist_tickers WHERE watchlist_id = %s", (watchlist_id,)
        )
        symbols = [r["symbol"] for r in symbols_rows]

        flagged = []
        for symbol in symbols:
            price_rows = lakebase.run_query(
                """
                SELECT daily_return_pct, snapshot_time FROM price_snapshots
                WHERE symbol = %s ORDER BY snapshot_time DESC LIMIT 1
                """,
                (symbol,),
            )
            if price_rows and price_rows[0]["daily_return_pct"] is not None:
                if abs(float(price_rows[0]["daily_return_pct"])) > 5:
                    flagged.append({
                        "symbol": symbol,
                        "reason": "price_move",
                        "daily_return_pct": price_rows[0]["daily_return_pct"],
                    })

            news_rows = lakebase.run_query(
                """
                SELECT title, published_at FROM news_articles
                WHERE symbol = %s AND published_at >= now() - (%s || ' days')::interval
                ORDER BY published_at DESC LIMIT 3
                """,
                (symbol, since_days),
            )
            for article in news_rows:
                flagged.append({
                    "symbol": symbol,
                    "reason": "news",
                    "title": article["title"],
                    "published_at": article["published_at"],
                })

        return {"since_days": since_days, "flagged": flagged}
    except Exception as e:
        logger.exception("Failed to flag notable changes")
        return {"status": "error", "message": str(e)}


# ============================================================
# Write tools
# ============================================================

@mcp.tool
def add_to_watchlist(symbol: str) -> dict:
    """
    Add a ticker to the current user's watchlist. Fetches and caches the
    company's profile if it isn't already stored.

    Args:
        symbol: Stock ticker symbol, e.g. "AAPL".

    Returns:
        A dict with status "success" and the symbol added, or "error"
        with a message on failure.
    """
    try:
        symbol = symbol.strip().upper()
        email = _get_end_user_email()
        user = _get_or_create_user(email)
        watchlist_id = _get_or_create_watchlist(user["user_id"])

        _ensure_company(symbol)

        lakebase.run_write(
            """
            INSERT INTO watchlist_tickers (watchlist_id, symbol)
            VALUES (%s, %s) ON CONFLICT (watchlist_id, symbol) DO NOTHING
            """,
            (watchlist_id, symbol),
        )
        return {"status": "success", "symbol": symbol, "user_email": email}
    except Exception as e:
        logger.exception(f"Failed to add {symbol!r} to watchlist")
        return {"status": "error", "message": str(e)}


@mcp.tool
def remove_from_watchlist(symbol: str) -> dict:
    """
    Remove a ticker from the current user's watchlist.

    Args:
        symbol: Stock ticker symbol to remove, e.g. "AAPL".

    Returns:
        A dict with status "success"/"not_found"/"error" and a message.
    """
    try:
        symbol = symbol.strip().upper()
        email = _get_end_user_email()
        user = _get_or_create_user(email)
        watchlist_id = _get_or_create_watchlist(user["user_id"])

        deleted = lakebase.run_write(
            "DELETE FROM watchlist_tickers WHERE watchlist_id = %s AND symbol = %s",
            (watchlist_id, symbol),
        )
        if not deleted:
            return {"status": "not_found", "message": f"{symbol} was not on the watchlist"}
        return {"status": "success", "symbol": symbol, "user_email": email}
    except Exception as e:
        logger.exception(f"Failed to remove {symbol!r} from watchlist")
        return {"status": "error", "message": str(e)}


@mcp.tool
def save_research_note(note_text: str, symbol: str | None = None) -> dict:
    """
    Save a research note for the current user, optionally tied to a ticker.

    Args:
        note_text: The note's content.
        symbol: Optional ticker symbol this note relates to.

    Returns:
        A dict with the saved note (note_id, symbol, note_text, created_at),
        or {"status": "error", "message": "..."} on failure.
    """
    try:
        note_text = note_text.strip()
        if not note_text:
            return {"status": "error", "message": "note_text is required"}

        symbol = symbol.strip().upper() if symbol else None
        email = _get_end_user_email()
        user = _get_or_create_user(email)

        rows = lakebase.run_write_returning(
            """
            INSERT INTO research_notes (user_id, symbol, note_text, created_at)
            VALUES (%s, %s, %s, now())
            RETURNING note_id, symbol, note_text, created_at
            """,
            (user["user_id"], symbol, note_text),
        )
        return rows[0]
    except Exception as e:
        logger.exception("Failed to save research note")
        return {"status": "error", "message": str(e)}


@mcp.tool
def save_analysis_report(symbols: list[str], report_text: str, report_type: str = "summary") -> dict:
    """
    Save a generated analysis report for the current user, tied to one or
    more tickers.

    Args:
        symbols: List of ticker symbols this report covers (e.g. a
            comparison report covers multiple; a single-ticker summary
            covers one).
        report_text: The report's content.
        report_type: A label for the kind of report, e.g. "summary",
            "comparison", "thesis_check" (default "summary").

    Returns:
        A dict with the saved report (report_id, symbols, report_type,
        report_text, created_at), or {"status": "error", "message": "..."}
        on failure.
    """
    try:
        symbols = [s.strip().upper() for s in symbols if s.strip()]
        report_text = report_text.strip()
        if not symbols or not report_text:
            return {"status": "error", "message": "symbols (non-empty list) and report_text are required"}

        email = _get_end_user_email()
        user = _get_or_create_user(email)

        rows = lakebase.run_write_returning(
            """
            INSERT INTO analysis_reports (user_id, symbols, report_type, report_text, created_at)
            VALUES (%s, %s, %s, %s, now())
            RETURNING report_id, symbols, report_type, report_text, created_at
            """,
            (user["user_id"], symbols, report_type, report_text),
        )
        return rows[0]
    except Exception as e:
        logger.exception("Failed to save analysis report")
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    if hasattr(mcp, "app") and mcp.app is not None:
        mcp.app.add_middleware(RequestContextMiddleware)

    port = int(os.getenv("DATABRICKS_APP_PORT", os.getenv("PORT", 8000)))
    mcp.run(transport="http", host="0.0.0.0", port=port)
