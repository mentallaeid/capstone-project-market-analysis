"""
sync_news.py

Standalone data-population script (like bulk_load_historical_prices.py
and embed_documents.py) - not part of either deployed app's app.yaml.
Reads the distinct set of watched tickers from watchlist_tickers, fetches
recent news for each from the Massive Stocks API, and upserts into
news_articles.

Run:
    python sync_news.py
"""

import json

import lakebase
import stocks_broker


def get_watched_symbols() -> list[str]:
    rows = lakebase.run_query("SELECT DISTINCT symbol FROM watchlist_tickers")
    return [row["symbol"] for row in rows]


def upsert_news_for_symbol(symbol: str, limit: int = 20) -> int:
    articles = stocks_broker.get_news(symbol, limit=limit)
    count = 0

    for article in articles:
        publisher = article.get("publisher") or {}
        lakebase.run_write(
            """
            INSERT INTO news_articles (
                id, symbol, title, description, source, published_at, url, payload, synced_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (id) DO UPDATE
                SET title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    source = EXCLUDED.source,
                    published_at = EXCLUDED.published_at,
                    url = EXCLUDED.url,
                    payload = EXCLUDED.payload,
                    synced_at = EXCLUDED.synced_at
            """,
            (
                str(article.get("id")),
                symbol,
                article.get("title", ""),
                article.get("description"),
                publisher.get("name"),
                article.get("published_utc"),
                article.get("article_url"),
                json.dumps(article),
            ),
        )
        count += 1

    return count


def main():
    symbols = get_watched_symbols()
    print(f"Found {len(symbols)} watched tickers: {symbols}")

    total = 0
    for symbol in symbols:
        try:
            synced = upsert_news_for_symbol(symbol)
            print(f"  {symbol}: synced {synced} articles")
            total += synced
        except Exception as exc:
            print(f"  {symbol}: failed to sync news ({exc})")
            continue

    print(f"Done. Synced {total} articles total.")


if __name__ == "__main__":
    main()
