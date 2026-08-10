"""
Stock Research Assistant - Databricks App.

Serves a small Flask API + frontend backed by Lakebase (Postgres).
Reads/writes go through lakebase.py; live market data comes from
stocks_broker.py (Massive Stocks API) only when needed (e.g. adding a
new ticker), not on every page load - price_snapshots is the source of
truth for anything already synced.

Run locally:
    python app.py
Deploy as a Databricks App using app.yaml.
"""

import logging
import os

from flask import Flask, jsonify, render_template, request

import lakebase
import stocks_broker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stock-research-app")

app = Flask(__name__)


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


@app.errorhandler(Exception)
def handle_exception(err):
    logger.exception("Unhandled exception while processing request")
    status_code = getattr(err, "code", 500)
    if not isinstance(status_code, int):
        status_code = 500
    return jsonify({"error": str(err)}), status_code


@app.route("/")
def index():
    return render_template("index.html")


def _current_user_email() -> str:
    """
    Databricks Apps inject the logged-in user's email via X-Forwarded-Email.
    Falls back to a placeholder for local development.
    """
    return request.headers.get("X-Forwarded-Email", "local-dev@example.com")


def _get_or_create_user(email: str) -> dict:
    rows = lakebase.run_query("SELECT user_id, email FROM users WHERE email = %s", (email,))
    if rows:
        return rows[0]
    rows = lakebase.run_write_returning(
        "INSERT INTO users (email) VALUES (%s) RETURNING user_id, email",
        (email,),
    )
    return rows[0]


def _get_or_create_watchlist(user_id: int) -> int:
    rows = lakebase.run_query(
        "SELECT watchlist_id FROM watchlists WHERE user_id = %s", (user_id,)
    )
    if rows:
        return rows[0]["watchlist_id"]
    rows = lakebase.run_write_returning(
        "INSERT INTO watchlists (user_id) VALUES (%s) RETURNING watchlist_id",
        (user_id,),
    )
    return rows[0]["watchlist_id"]


def _ensure_company(symbol: str) -> None:
    """Fetch and store a company's profile if we don't have it yet."""
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


# ============================================================
# Watchlist
# ============================================================

@app.route("/watchlist", methods=["GET"])
def get_watchlist():
    """The current user's watchlist, with each ticker's latest known price."""
    user = _get_or_create_user(_current_user_email())
    watchlist_id = _get_or_create_watchlist(user["user_id"])

    rows = lakebase.run_query(
        """
        SELECT wt.symbol, c.name, wt.added_at,
               ps.price, ps.daily_return_pct, ps.snapshot_time
        FROM watchlist_tickers wt
        LEFT JOIN companies c ON c.symbol = wt.symbol
        LEFT JOIN LATERAL (
            SELECT price, daily_return_pct, snapshot_time
            FROM price_snapshots
            WHERE symbol = wt.symbol
            ORDER BY snapshot_time DESC
            LIMIT 1
        ) ps ON true
        WHERE wt.watchlist_id = %s
        ORDER BY wt.symbol ASC
        """,
        (watchlist_id,),
    )
    return jsonify(rows)


@app.route("/watchlist", methods=["POST"])
def add_to_watchlist():
    """Add a ticker to the current user's watchlist."""
    data = request.get_json(force=True)
    symbol = (data.get("symbol") or "").strip().upper()
    if not symbol:
        return jsonify({"error": "symbol is required"}), 400

    user = _get_or_create_user(_current_user_email())
    watchlist_id = _get_or_create_watchlist(user["user_id"])

    _ensure_company(symbol)

    lakebase.run_write(
        """
        INSERT INTO watchlist_tickers (watchlist_id, symbol)
        VALUES (%s, %s)
        ON CONFLICT (watchlist_id, symbol) DO NOTHING
        """,
        (watchlist_id, symbol),
    )
    return jsonify({"symbol": symbol, "added": True}), 201


@app.route("/watchlist/<symbol>", methods=["DELETE"])
def remove_from_watchlist(symbol):
    symbol = symbol.strip().upper()
    user = _get_or_create_user(_current_user_email())
    watchlist_id = _get_or_create_watchlist(user["user_id"])

    deleted = lakebase.run_write(
        "DELETE FROM watchlist_tickers WHERE watchlist_id = %s AND symbol = %s",
        (watchlist_id, symbol),
    )
    if not deleted:
        return jsonify({"error": f"{symbol} is not on your watchlist"}), 404
    return jsonify({"symbol": symbol, "removed": True})


# ============================================================
# Companies / news
# ============================================================

@app.route("/companies/<symbol>", methods=["GET"])
def get_company(symbol):
    """Company profile, latest price, and recent news for one ticker."""
    symbol = symbol.strip().upper()

    company_rows = lakebase.run_query(
        "SELECT symbol, name, sector, description FROM companies WHERE symbol = %s",
        (symbol,),
    )
    if not company_rows:
        return jsonify({"error": f"No company data for {symbol}. Add it to a watchlist first."}), 404

    price_rows = lakebase.run_query(
        """
        SELECT price, daily_return_pct, moving_avg_7d, snapshot_time
        FROM price_snapshots WHERE symbol = %s
        ORDER BY snapshot_time DESC LIMIT 1
        """,
        (symbol,),
    )
    news_rows = lakebase.run_query(
        """
        SELECT id, title, description, source, published_at, url
        FROM news_articles WHERE symbol = %s
        ORDER BY published_at DESC LIMIT 10
        """,
        (symbol,),
    )

    return jsonify({
        "company": company_rows[0],
        "latest_price": price_rows[0] if price_rows else None,
        "recent_news": news_rows,
    })


# ============================================================
# Research notes
# ============================================================

@app.route("/research-notes", methods=["GET"])
def list_research_notes():
    symbol = request.args.get("symbol")
    user = _get_or_create_user(_current_user_email())

    if symbol:
        rows = lakebase.run_query(
            "SELECT note_id, symbol, note_text, created_at FROM research_notes "
            "WHERE user_id = %s AND symbol = %s ORDER BY created_at DESC",
            (user["user_id"], symbol.strip().upper()),
        )
    else:
        rows = lakebase.run_query(
            "SELECT note_id, symbol, note_text, created_at FROM research_notes "
            "WHERE user_id = %s ORDER BY created_at DESC",
            (user["user_id"],),
        )
    return jsonify(rows)


@app.route("/research-notes", methods=["POST"])
def create_research_note():
    data = request.get_json(force=True)
    symbol = (data.get("symbol") or "").strip().upper() or None
    note_text = (data.get("note_text") or "").strip()

    if not note_text:
        return jsonify({"error": "note_text is required"}), 400

    user = _get_or_create_user(_current_user_email())
    rows = lakebase.run_write_returning(
        "INSERT INTO research_notes (user_id, symbol, note_text, created_at) "
        "VALUES (%s, %s, %s, now()) RETURNING note_id, symbol, note_text, created_at",
        (user["user_id"], symbol, note_text),
    )
    return jsonify(rows[0]), 201


# ============================================================
# Analysis reports
# ============================================================

@app.route("/analysis-reports", methods=["GET"])
def list_analysis_reports():
    user = _get_or_create_user(_current_user_email())
    rows = lakebase.run_query(
        "SELECT report_id, symbols, report_type, report_text, created_at "
        "FROM analysis_reports WHERE user_id = %s ORDER BY created_at DESC",
        (user["user_id"],),
    )
    return jsonify(rows)


@app.route("/analysis-reports", methods=["POST"])
def create_analysis_report():
    data = request.get_json(force=True)
    symbols = [s.strip().upper() for s in (data.get("symbols") or []) if s.strip()]
    report_type = (data.get("report_type") or "summary").strip()
    report_text = (data.get("report_text") or "").strip()

    if not symbols or not report_text:
        return jsonify({"error": "symbols (non-empty list) and report_text are required"}), 400

    user = _get_or_create_user(_current_user_email())
    rows = lakebase.run_write_returning(
        "INSERT INTO analysis_reports (user_id, symbols, report_type, report_text, created_at) "
        "VALUES (%s, %s, %s, %s, now()) "
        "RETURNING report_id, symbols, report_type, report_text, created_at",
        (user["user_id"], symbols, report_type, report_text),
    )
    return jsonify(rows[0]), 201


if __name__ == '__main__':
    host = os.getenv('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_RUN_PORT', 8000))
    app.run(debug=True, host=host, port=port)
