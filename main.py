import asyncio
from datetime import datetime, timezone
from typing import Optional, Any
from collections import deque

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
    version="5.0.0",
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
# SIGNAL ENGINE V1 — NIFTY 50 + SENSEX ONLY
# =========================================================

SIGNAL_SYMBOLS = ("NIFTY 50", "SENSEX")
MAX_1M_CANDLES = 300
MAX_5M_CANDLES = 200

candles_1m = {
    s: deque(maxlen=MAX_1M_CANDLES)
    for s in SIGNAL_SYMBOLS
}
candles_5m = {
    s: deque(maxlen=MAX_5M_CANDLES)
    for s in SIGNAL_SYMBOLS
}
active_1m = {s: None for s in SIGNAL_SYMBOLS}
active_5m = {s: None for s in SIGNAL_SYMBOLS}

signals: dict[str, dict[str, Any]] = {}
signal_history = deque(maxlen=100)

# Daily CPR/Fib Pivot is intentionally not fabricated.
# It needs previous-day High/Low/Close bootstrap, which will be wired
# in the next module together with option data.
daily_levels = {
    s: {
        "ready": False,
        "pivot": None,
        "cpr": None,
        "fib": None,
        "reason": "Previous-day H/L/C bootstrap not connected yet.",
    }
    for s in SIGNAL_SYMBOLS
}


def _ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    out = sum(values[:period]) / period
    for v in values[period:]:
        out = v * k + out * (1 - k)
    return out


def _sma(values, period):
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def _rsi(values, period=14):
    if len(values) < period + 1:
        return None
    x = values[-(period + 1):]
    gains, losses = [], []
    for i in range(1, len(x)):
        d = x[i] - x[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains) / period
    al = sum(losses) / period
    if al == 0:
        return 100.0
    rs = ag / al
    return 100 - (100 / (1 + rs))


def _williams_r(candles, period=14):
    if len(candles) < period:
        return None
    x = candles[-period:]
    hh = max(c["high"] for c in x)
    ll = min(c["low"] for c in x)
    close = x[-1]["close"]
    if hh == ll:
        return -50.0
    return -100 * (hh - close) / (hh - ll)


def _atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        cur, prev = candles[i], candles[i - 1]
        tr = max(
            cur["high"] - cur["low"],
            abs(cur["high"] - prev["close"]),
            abs(cur["low"] - prev["close"]),
        )
        trs.append(tr)
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


def _price_action(candles):
    if len(candles) < 3:
        return "NEUTRAL"
    a, b, c = candles[-3], candles[-2], candles[-1]
    if c["high"] > b["high"] > a["high"] and c["low"] > b["low"] > a["low"]:
        return "BULLISH"
    if c["high"] < b["high"] < a["high"] and c["low"] < b["low"] < a["low"]:
        return "BEARISH"
    return "NEUTRAL"


def _breakout(candles):
    if len(candles) < 2:
        return "NONE"
    prev, cur = candles[-2], candles[-1]
    if cur["close"] > prev["high"]:
        return "BULLISH"
    if cur["close"] < prev["low"]:
        return "BEARISH"
    return "NONE"


def _bucket(epoch_seconds, minutes):
    size = minutes * 60
    return epoch_seconds - (epoch_seconds % size)


def _new_candle(bucket, price):
    return {
        "ts": bucket,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "ticks": 1,
    }


def _update_candle(candle, price):
    candle["high"] = max(candle["high"], price)
    candle["low"] = min(candle["low"], price)
    candle["close"] = price
    candle["ticks"] += 1


def _indicator_snapshot(symbol):
    one = list(candles_1m[symbol])
    five = list(candles_5m[symbol])

    c1 = [c["close"] for c in one]
    c5 = [c["close"] for c in five]

    return {
        "symbol": symbol,
        "one_minute": {
            "count": len(one),
            "ema9": _ema(c1, 9),
            "ema21": _ema(c1, 21),
            "rsi14": _rsi(c1, 14),
            "williams_r14": _williams_r(one, 14),
            "atr14": _atr(one, 14),
            "price_action": _price_action(one),
            "breakout": _breakout(one),
        },
        "five_minute": {
            "count": len(five),
            "ema9": _ema(c5, 9),
            "ema21": _ema(c5, 21),
            "ma20": _sma(c5, 20),
            "rsi14": _rsi(c5, 14),
            "price_action": _price_action(five),
        },
        "daily_levels": daily_levels[symbol],
        "vwap": {
            "ready": False,
            "reason": "True index VWAP needs usable traded volume; not fabricated.",
        },
        "order_flow": {
            "ready": False,
            "reason": "Depth/order-flow module not enabled yet.",
        },
        "options": {
            "ready": False,
            "reason": "Option LTP/OI/Greeks/VIX come in the next module.",
        },
    }


def _direction_score(snapshot, direction):
    one = snapshot["one_minute"]
    five = snapshot["five_minute"]

    score = 0
    reasons = []
    blockers = []

    # 5m EMA21 requires 21 completed 5m bars.
    if one["count"] < 22:
        blockers.append(f"1m warm-up {one['count']}/22")
    if five["count"] < 21:
        blockers.append(f"5m warm-up {five['count']}/21")

    if five["price_action"] == direction:
        score += 20
        reasons.append("5M price action")

    if five["ema9"] is not None and five["ema21"] is not None:
        ok = (
            five["ema9"] > five["ema21"]
            if direction == "BULLISH"
            else five["ema9"] < five["ema21"]
        )
        if ok:
            score += 15
            reasons.append("5M EMA 9/21")

    if one["ema9"] is not None and one["ema21"] is not None:
        ok = (
            one["ema9"] > one["ema21"]
            if direction == "BULLISH"
            else one["ema9"] < one["ema21"]
        )
        if ok:
            score += 15
            reasons.append("1M EMA 9/21")

    if one["breakout"] == direction:
        score += 20
        reasons.append("1M breakout")

    r = one["rsi14"]
    if r is not None:
        if direction == "BULLISH" and 55 <= r <= 78:
            score += 10
            reasons.append("RSI bullish zone")
        elif direction == "BEARISH" and 22 <= r <= 45:
            score += 10
            reasons.append("RSI bearish zone")

    wr = one["williams_r14"]
    if wr is not None:
        if direction == "BULLISH" and -50 <= wr <= -5:
            score += 10
            reasons.append("Williams %R bullish")
        elif direction == "BEARISH" and -95 <= wr <= -50:
            score += 10
            reasons.append("Williams %R bearish")

    if one["price_action"] == direction:
        score += 10
        reasons.append("1M price action")

    return score, reasons, blockers


async def _evaluate_signal(symbol):
    snap = _indicator_snapshot(symbol)

    bull_score, bull_reasons, bull_blockers = _direction_score(
        snap, "BULLISH"
    )
    bear_score, bear_reasons, bear_blockers = _direction_score(
        snap, "BEARISH"
    )

    if bull_score >= bear_score:
        direction = "CALL"
        score = bull_score
        reasons = bull_reasons
        blockers = bull_blockers
    else:
        direction = "PUT"
        score = bear_score
        reasons = bear_reasons
        blockers = bear_blockers

    if blockers:
        grade = "WARMING_UP"
        actionable = False
    elif score >= 80:
        grade = "A+"
        actionable = True
    elif score >= 70:
        grade = "STRONG"
        actionable = True
    elif score >= 60:
        grade = "WATCH"
        actionable = False
    else:
        grade = "NO_TRADE"
        actionable = False

    signal = {
        "symbol": symbol,
        "direction": direction,
        "score": score,
        "grade": grade,
        "actionable": actionable,
        "underlying_ltp": latest.get(symbol, {}).get("ltp"),
        "reasons": reasons,
        "blockers": blockers,
        "indicators": snap,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "SIGNAL_ONLY",
        "option_contract": None,
        "option_ltp": None,
        "entry": None,
        "stop_loss": None,
        "target_1": None,
        "target_2": None,
    }

    previous = signals.get(symbol)
    signals[symbol] = signal

    await broadcast({
        "type": "signal_update",
        "data": signal,
    })

    if (
        previous is None
        or previous.get("grade") != grade
        or previous.get("direction") != direction
    ):
        signal_history.appendleft(signal)
        await broadcast({
            "type": "signal_event",
            "data": signal,
        })


async def consume_signal_tick(symbol, price, received_at):
    if symbol not in SIGNAL_SYMBOLS:
        return

    try:
        epoch = int(
            datetime.fromisoformat(
                received_at.replace("Z", "+00:00")
            ).timestamp()
        )
    except Exception:
        epoch = int(datetime.now(timezone.utc).timestamp())

    closed = False

    for minutes, active, history in (
        (1, active_1m, candles_1m),
        (5, active_5m, candles_5m),
    ):
        bucket = _bucket(epoch, minutes)
        current = active[symbol]

        if current is None:
            active[symbol] = _new_candle(bucket, price)

        elif current["ts"] == bucket:
            _update_candle(current, price)

        elif bucket > current["ts"]:
            history[symbol].append(dict(current))
            active[symbol] = _new_candle(bucket, price)
            closed = True

    # Indicator calculation only at candle boundary, not on every tick.
    if closed:
        await _evaluate_signal(symbol)



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

                    # Strategy scans only NIFTY 50 + SENSEX.
                    # BANK NIFTY remains live on the dashboard.
                    if key in SIGNAL_SYMBOLS and item["ltp"] is not None:
                        await consume_signal_tick(
                            key,
                            item["ltp"],
                            now,
                        )


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
        "signal_engine": {
            "symbols": list(SIGNAL_SYMBOLS),
            "one_minute_candles": {
                s: len(candles_1m[s])
                for s in SIGNAL_SYMBOLS
            },
            "five_minute_candles": {
                s: len(candles_5m[s])
                for s in SIGNAL_SYMBOLS
            },
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
# SIGNAL API
# =========================================================

@app.get("/api/signals")
async def get_signals():
    return {
        "mode": "SIGNAL_ONLY",
        "signals": signals,
        "history": list(signal_history),
        "scans": list(SIGNAL_SYMBOLS),
    }


@app.get("/api/signals/{symbol}")
async def get_signal(symbol: str):
    aliases = {
        "NIFTY": "NIFTY 50",
        "NIFTY50": "NIFTY 50",
        "NIFTY 50": "NIFTY 50",
        "SENSEX": "SENSEX",
    }

    key = aliases.get(symbol.strip().upper())

    if not key:
        raise HTTPException(
            status_code=404,
            detail="V1 signal engine scans only NIFTY 50 and SENSEX.",
        )

    return {
        "signal": signals.get(key),
        "indicators": _indicator_snapshot(key),
        "one_minute_candles": list(candles_1m[key])[-50:],
        "five_minute_candles": list(candles_5m[key])[-50:],
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

    if signals:
        await websocket.send_json({
            "type": "signal_snapshot",
            "data": signals,
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
