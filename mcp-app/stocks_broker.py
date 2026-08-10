"""
Massive Stocks API adapter for the stock research capstone.

Mirrors the massive_broker.py / MassiveClient pattern used throughout the
Day 1-3 projects: a Databricks secret holds the API key, and every
function here is a thin wrapper returning clean dicts - no MCP decorators,
no Lakebase writes (that happens in the calling code, same separation of
concerns as alpaca_broker.py / weather_broker.py).
"""

import base64
import os

import requests
from databricks.sdk import WorkspaceClient

_w = WorkspaceClient()

_SECRET_SCOPE = os.environ.get("MASSIVE_SECRET_SCOPE", "massive")
_SECRET_KEY = os.environ.get("MASSIVE_SECRET_KEY", "api-key")
_BASE_URL = "https://api.massive.com"


def _api_key() -> str:
    """Fetch and base64-decode the Massive API key from the Databricks secret scope."""
    secret = _w.secrets.get_secret(scope=_SECRET_SCOPE, key=_SECRET_KEY)
    return base64.b64decode(secret.value).decode("utf-8")


def _get(path: str, params: dict | None = None) -> dict:
    """Shared GET helper - injects the API key, raises on HTTP errors."""
    params = dict(params or {})
    params["apiKey"] = _api_key()
    resp = requests.get(f"{_BASE_URL}{path}", params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def get_quote(symbol: str) -> dict:
    """
    Get the latest available price for a ticker (previous close aggregate).
    """
    symbol = symbol.strip().upper()
    data = _get(f"/v2/aggs/ticker/{symbol}/prev")

    if data.get("status") not in (None, "OK") or data.get("resultsCount") == 0:
        raise ValueError(f"No price data available for ticker: {symbol}")

    results = data.get("results") or []
    bar = results[0] if results else {}

    return {
        "symbol": symbol,
        "price": bar.get("c"),
        "open": bar.get("o"),
        "high": bar.get("h"),
        "low": bar.get("l"),
        "volume": bar.get("v"),
        "as_of": bar.get("t"),  # epoch ms, caller converts if needed
    }


def get_historical_prices(symbol: str, from_date: str, to_date: str) -> list[dict]:
    """
    Get daily aggregate bars for a ticker over a date range (YYYY-MM-DD).
    Intended for bulk historical loads (the Spark pipeline step), not
    single-quote lookups.
    """
    symbol = symbol.strip().upper()
    data = _get(f"/v2/aggs/ticker/{symbol}/range/1/day/{from_date}/{to_date}")

    if data.get("status") not in (None, "OK"):
        raise ValueError(f"Failed to fetch historical prices for {symbol}: {data}")

    bars = data.get("results") or []
    return [
        {
            "symbol": symbol,
            "date": bar.get("t"),  # epoch ms, caller converts to a date
            "open": bar.get("o"),
            "high": bar.get("h"),
            "low": bar.get("l"),
            "close": bar.get("c"),
            "volume": bar.get("v"),
        }
        for bar in bars
    ]


def get_company_profile(symbol: str) -> dict:
    """
    Get company fundamentals/profile: name, sector/industry classification,
    and description - the text used for company_profile embeddings.
    """
    symbol = symbol.strip().upper()
    data = _get(f"/v3/reference/tickers/{symbol}")

    if data.get("status") not in (None, "OK"):
        raise ValueError(f"No company profile found for ticker: {symbol}")

    result = data.get("results") or {}
    return {
        "symbol": symbol,
        "name": result.get("name"),
        "sic_description": result.get("sic_description"),  # closest available field to "sector/industry"
        "description": result.get("description"),
        "homepage_url": result.get("homepage_url"),
        "market_cap": result.get("market_cap"),
    }


def get_news(symbol: str, limit: int = 50) -> list[dict]:
    """
    Get recent news articles for a ticker, with sentiment insights.
    Same response shape/pattern as the Day 1-3 news pipelines.
    """
    symbol = symbol.strip().upper()
    data = _get(
        "/v2/reference/news",
        params={"ticker": symbol, "limit": limit, "order": "desc", "sort": "published_utc"},
    )
    return data.get("results") or []
