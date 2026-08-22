import asyncio
from datetime import datetime, timezone
from typing import Optional, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from neo_api_client import NeoAPI
from neo_api_client.websocket.feed import (
    WsToken,
    SFeedIndex,
)

from config import settings


# =========================================================
# APP
# =========================================================

app = FastAPI(
    title="King Bro Terminal API",
    version="4.2.0",
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.FRONTEND_URL],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================================
# REQUEST MODELS
# =========================================================

class TotpRequest(BaseModel):
    totp: str = Field(min_length=6, max_length=6)


# =========================================================
# GLOBAL STATE
# =========================================================

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


# =========================================================
# HELPERS
# =========================================================

async def broadcast(payload: dict):
    dead_clients = []

    for ws in list(browser_clients):
        try:
            await ws.send_json(payload)
        except Exception:
            dead_clients.append(ws)

    for ws in dead_clients:
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


def response_has_error(response):
    if response is None:
        return True

    if not isinstance(response, dict):
        return False

    if response.get("error") is True:
        return True

    if str(
        response.get(
            "status",
            ""
        )
    ).lower() in {
        "error",
        "failed",
        "failure",
    }:
        return True

    data = response.get("data")

    if isinstance(data, dict):

        if str(
            data.get(
                "status",
                ""
            )
        ).lower() in {
            "error",
            "failed",
            "failure",
        }:
            return True

    if (
        response.get("message")
        and not response.get("data")
    ):
        return True

    return False


def safe_api_message(response):
    if not isinstance(response, dict):
        return str(response)

    message = (
        response.get("message")
        or response.get("status")
        or response.get("error")
    )

    data = response.get("data")

    if isinstance(data, dict):
        message = (
            data.get("message")
            or data.get("status")
            or message
        )

    return str(
        message
        or "Unknown Kotak API error"
    )


def canonical_index_name(
    instrument_token: str,
    trading_symbol: str,
):
    combined = (
        f"{instrument_token} "
        f"{trading_symbol}"
    ).strip().lower()

    if "sensex" in combined:
        return "SENSEX"

    if (
        "nifty bank" in combined
        or "bank nifty" in combined
        or "banknifty" in combined
    ):
        return "BANK NIFTY"

    if "nifty 50" in combined:
        return "NIFTY 50"

    return None


# =========================================================
# AUTHENTICATION
# =========================================================

def authenticate_sync(totp: str):
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


    # -----------------------------------------------------
    # STEP 1 — TOTP LOGIN
    # -----------------------------------------------------

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


    # -----------------------------------------------------
    # STEP 2 — MPIN VALIDATE
    # -----------------------------------------------------

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


# =========================================================
# KOTAK INDEX LIVE FEED
# =========================================================

async def feed_loop():
    global neo_client

    backoff = 2

    while neo_client is not None:

        try:

            async with (
                neo_client
                .create_websocket()
            ) as ws:

                # -----------------------------------------
                # SUBSCRIBE INDEX FEED
                # -----------------------------------------

                await ws.subscribe_index([
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


                # -----------------------------------------
                # RECEIVE INDEX TICKS
                # -----------------------------------------

                async for message in ws:

                    if not isinstance(
                        message,
                        SFeedIndex
                    ):
                        continue


                    # -------------------------------------
                    # Convert Pydantic model -> dict
                    # -------------------------------------

                    try:
                        data = message.model_dump()
                    except Exception:
                        data = {}


                    instrument_token = str(
                        data.get(
                            "instrument_token",
                            ""
                        )
                        or ""
                    )


                    trading_symbol = str(
                        data.get(
                            "trading_symbol",
                            ""
                        )
                        or data.get(
                            "display_symbol",
                            ""
                        )
                        or data.get(
                            "index_name",
                            ""
                        )
                        or ""
                    )


                    key = canonical_index_name(
                        instrument_token,
                        trading_symbol,
                    )


                    if key is None:
                        continue


                    # -------------------------------------
                    # LTP
                    # -------------------------------------

                    ltp = (
                        data.get(
                            "last_traded_price"
                        )
                        or data.get("ltp")
                        or data.get(
                            "index_value"
                        )
                        or data.get(
                            "last_price"
                        )
                    )


                    # -------------------------------------
                    # CHANGE
                    # -------------------------------------

                    change = (
                        data.get("change")
                        or data.get(
                            "net_change"
                        )
                    )


                    # -------------------------------------
                    # PERCENT CHANGE
                    # -------------------------------------

                    percent_change = (
                        data.get(
                            "percentage_change"
                        )
                        or data.get(
                            "percent_change"
                        )
                        or data.get(
                            "per_change"
                        )
                    )


                    now = (
                        datetime
                        .now(
                            timezone.utc
                        )
                        .isoformat()
                    )


                    item = {
                        "key":
                            key,

                        "symbol":
                            trading_symbol
                            or instrument_token,

                        "instrument_token":
                            instrument_token,

                        "ltp":
                            number(
                                ltp
                            ),

                        "change":
                            number(
                                change
                            ),

                        "percent_change":
                            number(
                                percent_change
                            ),

                        "received_at":
                            now,
                    }


                    latest[key] = item


                    await set_status(
                        last_tick_at=now
                    )


                    await broadcast({
                        "type":
                            "tick",

                        "data":
                            item,
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


# =========================================================
# ROOT
# =========================================================

@app.get("/")
async def root():
    return {
        "app":
            "King Bro Terminal",

        "status":
            "online",

        "data_source":
            "Kotak Neo Index Feed",

        "mock_data":
            False,
    }


# =========================================================
# HEALTH
# =========================================================

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


# =========================================================
# CONNECT KOTAK
# =========================================================

@app.post(
    "/api/kotak/connect"
)
async def connect_kotak(
    body: TotpRequest
):
    global neo_client
    global feed_task


    if not body.totp.isdigit():
        raise HTTPException(
            status_code=400,

            detail={
                "message":
                    "TOTP must contain only digits."
            },
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


        # -----------------------------------------
        # Stop previous feed if one exists
        # -----------------------------------------

        if (
            feed_task
            and not feed_task.done()
        ):
            feed_task.cancel()


        # -----------------------------------------
        # Start fresh index feed
        # -----------------------------------------

        feed_task = (
            asyncio.create_task(
                feed_loop()
            )
        )


        return {
            "ok":
                True,

            "message":
                "Kotak authenticated. "
                "Index live feed starting.",
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


# =========================================================
# SNAPSHOT
# =========================================================

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


# =========================================================
# BROWSER WEBSOCKET
# =========================================================

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


    # -----------------------------------------
    # Send current connection status
    # -----------------------------------------

    await websocket.send_json({
        "type":
            "status",

        "data":
            status,
    })


    # -----------------------------------------
    # Send latest existing ticks
    # -----------------------------------------

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
