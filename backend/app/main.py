
import asyncio
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from neo_api_client import NeoAPI
from app.config import settings

app = FastAPI(title="King Bro Terminal API", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.FRONTEND_URL],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

INDEX_SPECS = [
    {"name": "NIFTY 50", "exchange_segment": "nse_cm", "instrument_token": "Nifty 50"},
    {"name": "SENSEX", "exchange_segment": "bse_cm", "instrument_token": "SENSEX"},
    {"name": "BANK NIFTY", "exchange_segment": "nse_cm", "instrument_token": "Nifty Bank"},
]

_client = None

def get_client():
    global _client
    if not settings.KOTAK_CONSUMER_KEY:
        raise RuntimeError("KOTAK_CONSUMER_KEY is missing in Render Environment.")
    if _client is None:
        _client = NeoAPI(
            consumer_key=settings.KOTAK_CONSUMER_KEY,
            environment=settings.KOTAK_ENVIRONMENT,
        )
    return _client

def to_float(v):
    if v is None:
        return None
    try:
        return float(str(v).replace(",", ""))
    except Exception:
        return None

def normalize_response(response):
    if hasattr(response, "model_dump"):
        response = response.model_dump()
    if isinstance(response, list):
        return response
    if isinstance(response, dict):
        for k in ("data", "quotes", "result"):
            if isinstance(response.get(k), list):
                return response[k]
    return []

def pick(row, *keys):
    for key in keys:
        if isinstance(row, dict) and row.get(key) is not None:
            return row[key]
    return None

def map_name(row):
    text = " ".join(str(row.get(k, "")) for k in (
        "display_symbol", "displaySymbol", "symbol", "trading_symbol",
        "exchange_token", "instrument_token"
    )).lower()
    if "sensex" in text:
        return "SENSEX"
    if "nifty bank" in text or "bank nifty" in text or "banknifty" in text:
        return "BANK NIFTY"
    if "nifty 50" in text or text.strip() == "nifty":
        return "NIFTY 50"
    return None

def normalize_row(row, fallback_name):
    ohlc = pick(row, "ohlc", "OHLC") or {}
    ltp = to_float(pick(row, "ltp", "last_traded_price", "lastTradedPrice", "lastPrice"))
    change = to_float(pick(row, "change", "net_change", "netChange"))
    pct = to_float(pick(row, "per_change", "percent_change", "percentageChange", "pChange"))
    close = to_float(pick(ohlc, "close", "c") or pick(row, "close", "previous_close", "prevClose"))

    if change is None and ltp is not None and close is not None:
        change = ltp - close
    if pct is None and change is not None and close not in (None, 0):
        pct = (change / close) * 100

    return {
        "name": fallback_name,
        "ltp": ltp,
        "change": change,
        "percent_change": pct,
        "open": to_float(pick(ohlc, "open", "o") or pick(row, "open")),
        "high": to_float(pick(ohlc, "high", "h") or pick(row, "high")),
        "low": to_float(pick(ohlc, "low", "l") or pick(row, "low")),
        "previous_close": close,
        "display_symbol": pick(row, "display_symbol", "displaySymbol", "symbol"),
        "exchange": pick(row, "exchange") or pick(row, "exchange_segment"),
    }

def fetch_quotes_sync():
    client = get_client()
    tokens = [
        {
            "instrument_token": x["instrument_token"],
            "exchange_segment": x["exchange_segment"],
        }
        for x in INDEX_SPECS
    ]

    response = client.quotes(instrument_tokens=tokens, quote_type="all")
    rows = normalize_response(response)

    by_name = {}
    for row in rows:
        name = map_name(row)
        if name:
            by_name[name] = row

    # If Kotak returns rows in request order but without display names, preserve that order.
    if len(by_name) < 3 and len(rows) == 3:
        for spec, row in zip(INDEX_SPECS, rows):
            by_name.setdefault(spec["name"], row)

    out = []
    for spec in INDEX_SPECS:
        row = by_name.get(spec["name"])
        if row is None:
            raise RuntimeError(f"Kotak quote response did not contain {spec['name']}. Raw row mapping unavailable.")
        item = normalize_row(row, spec["name"])
        item["exchange_segment"] = spec["exchange_segment"]
        item["instrument_token"] = spec["instrument_token"]
        out.append(item)
    return out

@app.get("/")
async def root():
    return {
        "app": "King Bro Terminal",
        "status": "online",
        "data_source": "Kotak Neo Quotes API",
        "mock_data": False,
    }

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "data_source": "kotak_neo_quotes_only",
        "totp_required": False,
        "mock_data": False,
        "consumer_key_configured": bool(settings.KOTAK_CONSUMER_KEY),
    }

@app.get("/api/market/quotes")
async def market_quotes():
    try:
        indices = await asyncio.to_thread(fetch_quotes_sync)
        return {
            "status": "ok",
            "source": "Kotak Neo Quotes API",
            "real_data": True,
            "mock_data": False,
            "server_time": datetime.now(timezone.utc).isoformat(),
            "indices": indices,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "source": "Kotak Neo",
                "error_type": type(exc).__name__,
                "message": str(exc),
                "mock_data_used": False,
            },
        )
