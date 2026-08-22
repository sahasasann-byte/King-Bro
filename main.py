import asyncio
from datetime import datetime, timezone
from typing import Optional, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from neo_api_client import NeoAPI
from neo_api_client.websocket.feed import WsToken, SFeedScrip

from config import settings


app = FastAPI(
    title="King Bro Terminal API",
    version="4.1.0",
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.FRONTEND_URL],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class TotpRequest(BaseModel):
    totp: str = Field(min_length=6, max_length=6)


# ---------------------------------------------------------
# GLOBAL STATE
# ---------------------------------------------------------

browser_clients: set[WebSocket] = set()

latest: dict[str, dict[str, Any]] = {}

status = {
    "broker_connected": False,
    "feed_connected": False,
    "last_tick_at": None,
    "last_error": None,
    "mock_data": False,
}

neo_client: Optional[NeoAPI] = None
feed_task: Optional[asyncio.Task] = None


# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------

async def broadcast(payload: dict):
    dead = []

    for ws in list(browser_clients):
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)

    for ws in dead:
        browser_clients.discard(ws)


async def set_status(**changes):
    status.update(changes)

    await broadcast({
        "type": "status",
        "data": status,
    })


def number(value):
    if value is None:
        return None

    try:
        return float(
            str(value).replace(",", "")
        )

    except Exception:
        return None


def canonical_symbol(symbol: str):
    s = (symbol or "").strip().lower()

    if "sensex" in s:
        return "SENSEX"

    if (
        "nifty bank" in s
        or "bank nifty" in s
        or "banknifty" in s
    ):
        return "BANK NIFTY"

    if (
        "nifty 50" in s
        or s == "nifty"
    ):
        return "NIFTY 50"

    return symbol or "UNKNOWN"


def response_has_error(response):
    """
    Kotak SDK may return a dict containing an API error
    instead of raising a Python exception.
    """

    if response is None:
        return True

    if not isinstance(response, dict):
        return False

    if response.get("error") is True:
        return True

    if str(response.get("status", "")).lower() in {
        "error",
        "failed",
        "failure",
    }:
        return True

    data = response.get("data")

    if isinstance(data, dict):
        if str(
            data.get("status", "")
        ).lower() in {
            "error",
            "failed",
            "failure",
        }:
            return True

    # Error-style payload with message and no session data
    if (
        response.get("message")
        and not response.get("data")
    ):
        return True

    return False


def safe_api_message(response):
    """
    Return only useful error information.
    Do not expose tokens/session secrets to browser.
    """

    if not isinstance(response, dict):
        return str(response)

    message = (
        response.get("message")
        or response.get("error")
        or response.get("status")
    )

    data = response.get("data")

    if isinstance(data, dict):
        message = (
            data.get("message")
            or data.get("status")
            or message
        )

    return str(
        message or "Unknown Kotak API error"
    )


# ---------------------------------------------------------
# AUTHENTICATION
# ---------------------------------------------------------

def authenticate_sync(totp: str):
    """
    Official Kotak flow:
    1. NeoAPI(consumer_key)
    2. totp_login()
    3. totp_validate()
    """

    required = {
        "KOTAK_CONSUMER_KEY":
            settings.KOTAK_CONSUMER_KEY,

        "KOTAK_MOBILE_NUMBER":
            settings.KOTAK_MOBILE_NUMBER,

        "KOTAK_UCC":
            settings.KOTAK_UCC,

        "KOTAK_MPIN":
            settings.KOTAK_MPIN,
    }

    missing = [
        key
        for key, value in required.items()
        if not value
    ]

    if missing:
        raise RuntimeError(
            "Missing Render variables: "
            + ", ".join(missing)
        )


    client = NeoAPI(
        consumer_key=
            settings.KOTAK_CONSUMER_KEY,

        environment=
            settings.KOTAK_ENVIRONMENT,
    )


    # STEP 1 — TOTP LOGIN
    login_response = client.totp_login(
        mobile_number=
            settings.KOTAK_MOBILE_NUMBER,

        ucc=
            settings.KOTAK_UCC,

        totp=
            totp,
    )


    if response_has_error(
        login_response
    ):
        raise RuntimeError(
            "TOTP login failed: "
            + safe_api_message(
                login_response
            )
        )


    # STEP 2 — MPIN VALIDATION
    validate_response = (
        client.totp_validate(
            mpin=
                settings.KOTAK_MPIN
        )
    )


    if response_has_error(
        validate_response
    ):
        raise RuntimeError(
            "MPIN validation failed: "
            + safe_api_message(
                validate_response
            )
        )


    return client


# ---------------------------------------------------------
# KOTAK LIVE FEED
# ---------------------------------------------------------

async def feed_loop():
    global neo_client

    backoff = 2

    while neo_client is not None:

        try:

            async with (
                neo_client
                .create_websocket()
            ) as ws:

                await ws.subscribe_scrips([
                    WsToken(
                        "nse_cm",
                        "Nifty 50",
                    ),

                    WsToken(
                        "nse_cm",
                        "Nifty Bank",
                    ),

                    WsToken(
                        "bse_cm",
                        "SENSEX",
                    ),
                ])


                await set_status(
                    feed_connected=True,
                    last_error=None,
                )


                async for message in ws:

                    if not isinstance(
                        message,
                        SFeedScrip,
                    ):
                        continue


                    symbol = (
                        getattr(
                            message,
                            "trading_symbol",
                            None,
                        )
                        or getattr(
                            message,
                            "display_symbol",
                            None,
                        )
                        or str(
                            getattr(
                                message,
                                "instrument_token",
                                "",
                            )
                        )
                    )


                    key = canonical_symbol(
                        symbol
                    )


                    if key not in (
                        "NIFTY 50",
                        "SENSEX",
                        "BANK NIFTY",
                    ):
                        continue


                    now = (
                        datetime
                        .now(
                            timezone.utc
                        )
                        .isoformat()
                    )


                    item = {
                        "key": key,

                        "symbol": symbol,

                        "ltp": number(
                            getattr(
                                message,
                                "last_traded_price",
                                None,
                            )
                        ),

                        "change": number(
                            getattr(
                                message,
                                "change",
                                None,
                            )
                        ),

                        "percent_change":
                            number(
                                getattr(
                                    message,
                                    "percentage_change",
                                    None,
                                )
                                or getattr(
                                    message,
                                    "percent_change",
                                    None,
                                )
                                or getattr(
                                    message,
                                    "per_change",
                                    None,
                                )
                            ),

                        "received_at": now,
                    }


                    latest[key] = item


                    await set_status(
                        last_tick_at=now
                    )


                    await broadcast({
                        "type": "tick",
                        "data": item,
                    })


                backoff = 2


        except asyncio.CancelledError:
            return


        except Exception as exc:

            await set_status(
                feed_connected=False,

                last_error=
                    f"{type(exc).__name__}: "
                    f"{exc}",
            )


            await asyncio.sleep(
                backoff
            )

            backoff = min(
                backoff * 2,
                30,
            )


# ---------------------------------------------------------
# API ROUTES
# ---------------------------------------------------------

@app.get("/")
async def root():
    return {
        "app":
            "King Bro Terminal",

        "status":
            "online",

        "mock_data":
            False,
    }


@app.get("/health")
async def health():
    return {
        "status":
            "ok",

        **status,

        "configured": {
            "consumer_key":
                bool(
                    settings
                    .KOTAK_CONSUMER_KEY
                ),

            "mobile":
                bool(
                    settings
                    .KOTAK_MOBILE_NUMBER
                ),

            "ucc":
                bool(
                    settings
                    .KOTAK_UCC
                ),

            "mpin":
                bool(
                    settings
                    .KOTAK_MPIN
                ),
        },
    }


@app.post(
    "/api/kotak/connect"
)
async def connect_kotak(
    body: TotpRequest
):
    global neo_client
    global feed_task


    # TOTP must be exactly six digits
    if not body.totp.isdigit():
        raise HTTPException(
            status_code=400,
            detail=
                "TOTP must contain only digits.",
        )


    try:

        authenticated_client = (
            await asyncio.to_thread(
                authenticate_sync,
                body.totp,
            )
        )


        neo_client = (
            authenticated_client
        )


        await set_status(
            broker_connected=True,
            feed_connected=False,
            last_error=None,
        )


        # Stop previous feed task
        if (
            feed_task
            and not feed_task.done()
        ):
            feed_task.cancel()


        feed_task = (
            asyncio.create_task(
                feed_loop()
            )
        )


        return {
            "ok": True,

            "message":
                "Kotak authenticated. "
                "Live feed starting.",
        }


    except Exception as exc:

        neo_client = None


        await set_status(
            broker_connected=False,
            feed_connected=False,

            last_error=
                f"{type(exc).__name__}: "
                f"{exc}",
        )


        raise HTTPException(
            status_code=400,

            detail={
                "error_type":
                    type(exc).__name__,

                "message":
                    str(exc),
            },
        )


@app.get(
    "/api/market/snapshot"
)
async def market_snapshot():

    return {
        "status":
            status,

        "items":
            list(
                latest.values()
            ),
    }


# ---------------------------------------------------------
# BROWSER WEBSOCKET
# ---------------------------------------------------------

@app.websocket(
    "/ws/market"
)
async def browser_market_ws(
    websocket: WebSocket
):

    await websocket.accept()

    browser_clients.add(
        websocket
    )


    await websocket.send_json({
        "type":
            "status",

        "data":
            status,
    })


    if latest:

        await websocket.send_json({
            "type":
                "snapshot",

            "data":
                list(
                    latest.values()
                ),
        })


    try:

        while True:
            await websocket.receive_text()


    except WebSocketDisconnect:

        browser_clients.discard(
            websocket
        )


    except Exception:

        browser_clients.discard(
            websocket
        )
