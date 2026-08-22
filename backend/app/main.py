import asyncio

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from neo_api_client import NeoAPI

from app.config import settings


app = FastAPI(
    title="King Bro Terminal API",
    version="3.1.0"
)


# --------------------------------------------------
# CORS
# --------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.FRONTEND_URL],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --------------------------------------------------
# KOTAK CLIENT
# --------------------------------------------------

_client = None


def get_client():

    global _client

    if not settings.KOTAK_CONSUMER_KEY:
        raise RuntimeError(
            "KOTAK_CONSUMER_KEY is missing in Render Environment"
        )

    if _client is None:

        _client = NeoAPI(
            consumer_key=settings.KOTAK_CONSUMER_KEY,
            environment=settings.KOTAK_ENVIRONMENT,
        )

    return _client


# --------------------------------------------------
# HEALTH CHECK
# --------------------------------------------------

@app.get("/")
async def root():

    return {
        "app": "King Bro Terminal",
        "status": "online",
        "data_source": "Kotak Neo",
        "mock_data": False
    }


@app.get("/health")
async def health():

    return {
        "status": "ok",
        "data_source": "kotak_neo_quotes_only",
        "totp_required": False,
        "mock_data": False,
        "consumer_key_configured":
            bool(settings.KOTAK_CONSUMER_KEY)
    }


# --------------------------------------------------
# FETCH KOTAK RAW MARKET DATA
# --------------------------------------------------

def fetch_raw_quotes():

    client = get_client()

    instruments = [

        {
            "instrument_token": "Nifty 50",
            "exchange_segment": "nse_cm"
        },

        {
            "instrument_token": "Nifty Bank",
            "exchange_segment": "nse_cm"
        },

        {
            "instrument_token": "SENSEX",
            "exchange_segment": "bse_cm"
        }

    ]

    response = client.quotes(
        instrument_tokens=instruments,
        quote_type="all"
    )

    return response


# --------------------------------------------------
# RAW TEST ENDPOINT
# --------------------------------------------------

@app.get("/api/market/raw")
async def market_raw():

    try:

        response = await asyncio.to_thread(
            fetch_raw_quotes
        )

        return {
            "status": "ok",
            "source": "Kotak Neo",
            "mock_data": False,
            "response": response
        }

    except Exception as exc:

        raise HTTPException(
            status_code=502,
            detail={
                "source": "Kotak Neo",
                "error_type":
                    type(exc).__name__,
                "message":
                    str(exc),
                "mock_data_used": False
            }
        )
