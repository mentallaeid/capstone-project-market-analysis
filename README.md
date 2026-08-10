# AI Stock Market Research Assistant - Capstone

Users track a personal watchlist, ask questions about tickers, and have
an agent pull real market data, summarize fundamentals/news, and log
research notes and analysis reports on their behalf.

## Architecture

```
User (browser)                     User (chat)
     |                                   |
     v                                   v
Flask App (app.py)              Agent Bricks agent (Custom LLM)
- watchlist CRUD                       |  MCP tool calls
- company/news views                   v
- notes/reports views          mcp_server.py (FastMCP, Databricks App)
     |                          - read tools (price, news, semantic search)
     |                          - write tools (watchlist, notes, reports)
     v                                   |
              Lakebase (Postgres)  <-----+
    users, watchlists, watchlist_tickers, companies,
    price_snapshots, news_articles, company_documents,
    research_notes, analysis_reports, document_embeddings (pgvector)
     ^                                   ^
     |                                   |
stocks_broker.py                embed_documents.py
(Massive Stocks API,            (sentence-transformers,
 live reads on-demand)           chunk + embed all 4 text sources)
     ^
     |
Spark pipeline (bulk_load_historical_prices.py)
- distributes historical price fetches across executors
- computes daily_return_pct / moving_avg_7d via window functions
- writes to Lakebase via psycopg2 (not spark.write.jdbc)
```

## Third-party API and auth

- **API:** [Massive Stocks API](https://massive.com/docs/rest/stocks/overview)
- **Auth:** API key, stored as a Databricks secret (scope `database`, key
  `massive-api-key`), fetched and base64-decoded via
  `WorkspaceClient().secrets.get_secret(...)` - same pattern as
  `alpaca_broker.py`/`massive_broker.py` from Day 1-3.
- **Endpoints used:** ticker overview (company profile), aggregates/prev
  (latest quote), aggregates/range (historical bars), reference/news.

## Lakebase schema (10 tables)

The 8 named in the assignment, plus two needed to actually satisfy the
context-engineering requirement:

| Table | Purpose |
|---|---|
| `users` | One row per identified user (email) |
| `watchlists` | One watchlist per user |
| `watchlist_tickers` | Symbols on each watchlist |
| `companies` | Profile/fundamentals per symbol (embedded text: `description`) |
| `price_snapshots` | Time-series price + `daily_return_pct` + `moving_avg_7d` (written by the Spark pipeline) |
| `news_articles` | Synced news per symbol (embedded text: `description`) |
| `company_documents` (added) | Filings excerpts / earnings summaries (embedded text: `document_text`) |
| `research_notes` | User/agent-saved notes tied to a ticker |
| `analysis_reports` | User/agent-saved reports, covering one or more tickers |
| `document_embeddings` (added) | Shared pgvector table for all 4 embedded text sources, tagged by `source_type` |

## Unstructured data + embeddings

Four text sources feed one shared embeddings table:
`companies.description`, `news_articles.description`,
`company_documents.document_text` (filings), and (`earnings_summary`
document type). `embed_documents.py` chunks (800 chars, 100 overlap,
sliding window) and embeds each with
`sentence-transformers/all-MiniLM-L6-v2` (384-dim), writing to
`document_embeddings` via `psycopg2.extras.execute_values` with a direct
`%s::vector` cast, indexed with HNSW (`vector_cosine_ops`) for retrieval.

## Spark pipeline

`bulk_load_historical_prices.py` (Databricks notebook):
1. Reads the distinct set of watched tickers from `watchlist_tickers`.
2. Distributes one Massive API historical-bars call per ticker across
   executors via `rdd.mapPartitions`.
3. Computes `daily_return_pct` (vs. prior close) and `moving_avg_7d`
   with Spark window functions partitioned by symbol.
4. Collects to the driver and writes to `price_snapshots` via
   `psycopg2`/`execute_values` (not `spark.write.jdbc`, which isn't
   reliable against Lakebase in this environment).

## Databricks App (frontend)

`app.py` + `templates/index.html`: add/remove watchlist tickers, view a
company's profile/price/news, add research notes, view saved analysis
reports. User identity comes from the `X-Forwarded-Email` header
Databricks Apps inject.

## AI agent

`mcp_server.py` (a second Databricks App, name prefixed `mcp-` for
direct AI Playground/Agent Bricks discovery) exposes 9 tools:

| Tool | Type |
|---|---|
| `get_price_history` | read |
| `compare_tickers` | read |
| `search_context` | read (semantic) |
| `get_recent_news` | read |
| `flag_notable_changes` | read |
| `add_to_watchlist` | write |
| `remove_from_watchlist` | write |
| `save_research_note` | write |
| `save_analysis_report` | write |

Full system prompt and setup steps: see `agent_bricks_setup.md`.

## Setup steps

1. Run `capstone_schema.sql` in the Lakebase SQL Editor (requires the
   `pgvector` extension enabled). Grant your app's role access to all
   10 tables.
2. Store the Massive API key as a Databricks secret
   (`database`/`massive-api-key`).
3. Deploy `app.py` (+ `requirements.txt`, `app.yaml`, `templates/index.html`)
   as a Databricks App.
4. Run `bulk_load_historical_prices.py` as a notebook/job to populate
   `price_snapshots`.
5. Sync companies/news into `companies`/`news_articles` (via the Flask
   app adding tickers, which calls `stocks_broker.get_company_profile`,
   or a dedicated sync script).
6. Run `embed_documents.py` to populate `document_embeddings`.
7. Deploy `mcp_server.py` (+ its own `requirements.txt`/`app.yaml`) as a
   Databricks App named `mcp-stock-research`.
8. Create the Agent Bricks agent per `agent_bricks_setup.md`, add the
   MCP server's 9 tools, paste the system prompt.
9. Test with the 5 demo questions in `agent_bricks_setup.md`; paste
   transcripts/screenshots here once run.

## Known limitations / future improvements

- `flag_notable_changes` uses a fixed lookback window (`since_days`),
  not a true "since your last visit" timestamp - would need a
  `users.last_seen_at` column and an update-on-request hook to do this
  properly.
- `company_documents` (filings/earnings) has no ingestion script yet in
  this writeup - would need its own sync step, similar to
  `stocks_broker.get_news`, pulling from a filings/earnings source.
- `search_context`'s per-symbol filter joins across three tables
  (`news_articles`, `company_documents`, plus a direct match) - fine at
  this scale, but would need a denormalized `symbol` column on
  `document_embeddings` if the dataset grew much larger.
- No caching on `stocks_broker` calls - `_ensure_company` avoids
  re-fetching a profile once cached, but nothing throttles repeated
  price/news calls beyond what's already synced.
- The agent's "offer to save, don't save automatically" guardrail
  depends on the LLM correctly reading confirmation language - worth
  testing with informal phrasings ("yeah do that"), not just exact
  matches like "save that."
