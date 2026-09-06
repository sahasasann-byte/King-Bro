import asyncio
import json
import os
import re
import urllib.parse
import urllib.request
from collections import deque
from datetime import datetime, timezone, date, time as dt_time
from typing import Any, Optional
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from neo_api_client import NeoAPI
from neo_api_client.websocket.feed import WsToken, SFeedIndex, SFeedScrip

APP_VERSION = "6.0.0"
IST = ZoneInfo("Asia/Kolkata")

app = FastAPI(
    title="KING BRO Telegram Original V6 Daily Auto",
    version=APP_VERSION,
)

# =========================================================
# ENV
# =========================================================
KOTAK_CONSUMER_KEY = os.getenv("KOTAK_CONSUMER_KEY", "").strip()
KOTAK_MOBILE_NUMBER = os.getenv("KOTAK_MOBILE_NUMBER", "").strip()
KOTAK_UCC = os.getenv("KOTAK_UCC", "").strip()
KOTAK_MPIN = os.getenv("KOTAK_MPIN", "").strip()
KOTAK_ENVIRONMENT = os.getenv("KOTAK_ENVIRONMENT", "prod").strip()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()

RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
PUBLIC_URL = (
    os.getenv("PUBLIC_URL", "").strip().rstrip("/")
    or RENDER_EXTERNAL_URL
)

# Durable candle storage. Render Free local /tmp is NOT durable.
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
STATE_GIST_ID = os.getenv("STATE_GIST_ID", "").strip()
STATE_GIST_FILENAME = os.getenv(
    "STATE_GIST_FILENAME",
    "kingbro_original_state.json",
).strip()

# One-time migration source. Keep old service alive until /status confirms restore.
BOOTSTRAP_URL = os.getenv("BOOTSTRAP_URL", "").strip().rstrip("/")

# =========================================================
# MARKET TIME / STRATEGY LOCK
# =========================================================
FEED_START = dt_time(9, 0)
SIGNAL_START = dt_time(9, 30)
SIGNAL_END = dt_time(15, 30)
FEED_END = dt_time(15, 40)

SIGNAL_SYMBOLS = ("NIFTY 50", "SENSEX")

# Exact old index strategy warm-up requirements.
INDEX_NEED_1M = 22
INDEX_NEED_5M = 21
INDEX_NEED_15M = 9

MAX_1M = 300
MAX_5M = 200
MAX_15M = 200

# Exact old public signal thresholds.
A_PLUS_SCORE = 80
STRONG_SCORE = 70
WATCH_SCORE = 60

# Exact old option quality gate.
OPTION_MIN_LIQUIDITY = 60

ALERT_COOLDOWN_SECONDS = 15 * 60

# =========================================================
# OLD V7.1/V7.3 STOCK UNIVERSE
# =========================================================
STOCK_UNIVERSE = (
    "RELIANCE", "HDFCBANK", "ICICIBANK", "SBIN", "AXISBANK",
    "KOTAKBANK", "INDUSINDBK", "BAJFINANCE", "TATAMOTORS", "M&M",
    "MARUTI", "EICHERMOT", "TVSMOTOR", "TATASTEEL", "HINDALCO",
    "JSWSTEEL", "ADANIENT", "ADANIPORTS", "LT", "BEL",
    "HAL", "BHEL", "RVNL", "IRFC", "PFC",
    "RECLTD", "POWERGRID", "NTPC", "TATAPOWER", "COALINDIA",
    "ONGC", "BPCL", "IOC", "ITC", "TRENT",
    "DLF", "INFY", "TCS", "BHARTIARTL", "SUNPHARMA",
)

STOCK_NEED_1M = 22
STOCK_NEED_5M = 9
STOCK_NEED_15M = 3
STOCK_MAX_1M = 180
STOCK_MAX_5M = 120
STOCK_MAX_15M = 80

# =========================================================
# STATE
# =========================================================
neo_client: Optional[NeoAPI] = None
index_feed_task: Optional[asyncio.Task] = None
stock_feed_task: Optional[asyncio.Task] = None

latest: dict[str, dict[str, Any]] = {}

candles_1m = {s: deque(maxlen=MAX_1M) for s in SIGNAL_SYMBOLS}
candles_5m = {s: deque(maxlen=MAX_5M) for s in SIGNAL_SYMBOLS}
candles_15m = {s: deque(maxlen=MAX_15M) for s in SIGNAL_SYMBOLS}
active_1m = {s: None for s in SIGNAL_SYMBOLS}
active_5m = {s: None for s in SIGNAL_SYMBOLS}
active_15m = {s: None for s in SIGNAL_SYMBOLS}

daily_levels = {
    s: {"ready": False}
    for s in SIGNAL_SYMBOLS
}

stock_token_map: dict[str, dict[str, str]] = {}
stock_latest: dict[str, dict[str, Any]] = {}
stock_candles_1m = {
    s: deque(maxlen=STOCK_MAX_1M)
    for s in STOCK_UNIVERSE
}
stock_candles_5m = {
    s: deque(maxlen=STOCK_MAX_5M)
    for s in STOCK_UNIVERSE
}
stock_candles_15m = {
    s: deque(maxlen=STOCK_MAX_15M)
    for s in STOCK_UNIVERSE
}
stock_active_1m = {s: None for s in STOCK_UNIVERSE}
stock_active_5m = {s: None for s in STOCK_UNIVERSE}
stock_active_15m = {s: None for s in STOCK_UNIVERSE}

index_alert_cache: dict[str, float] = {}
stock_alert_cache: dict[str, float] = {}

state_lock = asyncio.Lock()

runtime = {
    "broker_connected": False,
    "index_feed_connected": False,
    "index_scan_enabled": True,
    "stock_scan_enabled": False,
    "stock_feed_connected": False,
    "stock_resolved": 0,
    "stock_unresolved": 0,
    "last_login_at": None,
    "last_tick_at": None,
    "last_index_signal_at": None,
    "last_stock_signal_at": None,
    "last_index_blocker": None,
    "last_error": None,
    "stock_error": None,
    "process_started_at": datetime.now(timezone.utc).isoformat(),
    "state_source": None,
    "last_state_save_at": None,
    "last_state_restore_at": None,
}

# =========================================================
# MODELS
# =========================================================
class TotpRequest(BaseModel):
    totp: str = Field(min_length=6, max_length=6)

# =========================================================
# GENERIC HELPERS
# =========================================================
def number(value):
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ""))
    except Exception:
        return None


def normalise_rows(response):
    if response is None:
        return []
    if isinstance(response, list):
        return response
    if isinstance(response, dict):
        data = response.get("data")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
    return []


def response_has_error(response):
    if response is None:
        return True
    if not isinstance(response, dict):
        return False
    if response.get("error") is True:
        return True
    if response.get("success") is False:
        return True
    status = str(response.get("status") or "").lower()
    return status in {"error", "failed", "failure"}


def safe_api_message(response):
    if response is None:
        return "empty response"
    if isinstance(response, dict):
        for key in ("message", "msg", "error", "error_description", "status"):
            if response.get(key):
                return str(response.get(key))
        data = response.get("data")
        if isinstance(data, dict):
            for key in ("message", "msg", "error", "status"):
                if data.get(key):
                    return str(data.get(key))
    return str(response)[:500]


def ist_now():
    return datetime.now(IST)


def is_market_weekday(now=None):
    now = now or ist_now()
    return now.weekday() < 5


def signal_window_open(now=None):
    now = now or ist_now()
    t = now.time().replace(tzinfo=None)
    return (
        is_market_weekday(now)
        and SIGNAL_START <= t <= SIGNAL_END
    )


def feed_window_open(now=None):
    now = now or ist_now()
    t = now.time().replace(tzinfo=None)
    return (
        is_market_weekday(now)
        and FEED_START <= t <= FEED_END
    )


def canonical_index_name(instrument_token, trading_symbol):
    combined = f"{instrument_token} {trading_symbol}".strip().lower()
    if "sensex" in combined:
        return "SENSEX"
    if "nifty 50" in combined:
        return "NIFTY 50"
    # Safe fallback for SDK variants that expose only NIFTY.
    if "nifty" in combined and "bank" not in combined:
        return "NIFTY 50"
    return None


def bucket_start(epoch_seconds, minutes):
    size = minutes * 60
    return int(epoch_seconds) - (int(epoch_seconds) % size)


def new_candle(bucket, price):
    return {
        "ts": bucket,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "ticks": 1,
    }


def update_candle(candle, price):
    candle["high"] = max(float(candle["high"]), price)
    candle["low"] = min(float(candle["low"]), price)
    candle["close"] = price
    candle["ticks"] = int(candle.get("ticks") or 0) + 1


# =========================================================
# TELEGRAM
# =========================================================
def telegram_keyboard():
    return {
        "keyboard": [
            ["/status", "/loginhelp"],
            ["/indexon", "/indexoff"],
            ["/stockon", "/stockoff"],
            ["/save", "/test"],
            ["/help"],
        ],
        "resize_keyboard": True,
        "is_persistent": True,
    }


def telegram_api(method, payload=None):
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN missing")

    url = (
        f"https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}/{method}"
    )

    body = urllib.parse.urlencode(payload or {}).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type":
                "application/x-www-form-urlencoded"
        },
    )

    with urllib.request.urlopen(request, timeout=15) as response:
        data = json.loads(
            response.read().decode("utf-8")
        )

    if not data.get("ok"):
        raise RuntimeError(
            f"Telegram API error: {data}"
        )

    return data


async def telegram_send(message, with_keyboard=False):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[TELEGRAM_DISABLED]", flush=True)
        return False

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": "true",
    }

    if with_keyboard:
        payload["reply_markup"] = json.dumps(
            telegram_keyboard()
        )

    last_error = None

    for attempt in range(1, 4):
        try:
            await asyncio.to_thread(
                telegram_api,
                "sendMessage",
                payload,
            )
            return True
        except Exception as exc:
            last_error = exc
            if attempt < 3:
                await asyncio.sleep(1.25)

    print(
        f"[TELEGRAM_SEND_FAILED] "
        f"{type(last_error).__name__}: {last_error}",
        flush=True,
    )
    return False


async def delete_telegram_message(chat_id, message_id):
    try:
        await asyncio.to_thread(
            telegram_api,
            "deleteMessage",
            {
                "chat_id": str(chat_id),
                "message_id": str(message_id),
            },
        )
    except Exception:
        pass


async def setup_telegram():
    if not (
        TELEGRAM_BOT_TOKEN
        and PUBLIC_URL
    ):
        return

    payload = {
        "url": f"{PUBLIC_URL}/telegram/webhook",
        "drop_pending_updates": "false",
    }

    if TELEGRAM_WEBHOOK_SECRET:
        payload["secret_token"] = (
            TELEGRAM_WEBHOOK_SECRET
        )

    await asyncio.to_thread(
        telegram_api,
        "setWebhook",
        payload,
    )

    commands = [
        {"command": "login", "description": "Login: /login 123456"},
        {"command": "status", "description": "Bot/feed/candle readiness"},
        {"command": "indexon", "description": "Enable NIFTY/SENSEX signals"},
        {"command": "indexoff", "description": "Pause NIFTY/SENSEX signals"},
        {"command": "stockon", "description": "Enable old 40-stock scanner"},
        {"command": "stockoff", "description": "Pause stock scanner"},
        {"command": "save", "description": "Save candle history now"},
        {"command": "test", "description": "Telegram test"},
        {"command": "help", "description": "Show controls"},
    ]

    await asyncio.to_thread(
        telegram_api,
        "setMyCommands",
        {"commands": json.dumps(commands)},
    )


# =========================================================
# DURABLE GIST STATE
# =========================================================
def gist_request(method, path, payload=None):
    if not (
        GITHUB_TOKEN
        and STATE_GIST_ID
    ):
        raise RuntimeError(
            "Gist persistence not configured"
        )

    url = f"https://api.github.com{path}"

    data = (
        None
        if payload is None
        else json.dumps(payload).encode("utf-8")
    )

    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization":
                f"Bearer {GITHUB_TOKEN}",
            "Accept":
                "application/vnd.github+json",
            "User-Agent":
                "kingbro-telegram-original-v6",
            "X-GitHub-Api-Version":
                "2022-11-28",
            "Content-Type":
                "application/json",
        },
    )

    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(
            response.read().decode("utf-8")
        )


def serialisable_state():
    return {
        "version": APP_VERSION,
        "saved_at": datetime.now(timezone.utc).isoformat(),

        # Keep enough history for the ORIGINAL index engine
        # (22x1m, 21x5m, 9x15m) to be ready immediately next morning.
        "candles_1m": {
            s: list(candles_1m[s])
            for s in SIGNAL_SYMBOLS
        },
        "candles_5m": {
            s: list(candles_5m[s])
            for s in SIGNAL_SYMBOLS
        },
        "candles_15m": {
            s: list(candles_15m[s])
            for s in SIGNAL_SYMBOLS
        },

        # Compact stock history. Used only if STOCK scan was enabled.
        "stock_candles_1m": {
            s: list(stock_candles_1m[s])[-60:]
            for s in STOCK_UNIVERSE
        },
        "stock_candles_5m": {
            s: list(stock_candles_5m[s])[-30:]
            for s in STOCK_UNIVERSE
        },
        "stock_candles_15m": {
            s: list(stock_candles_15m[s])[-12:]
            for s in STOCK_UNIVERSE
        },
    }


def save_state_sync():
    if not (
        GITHUB_TOKEN
        and STATE_GIST_ID
    ):
        return False

    content = json.dumps(
        serialisable_state(),
        separators=(",", ":"),
    )

    gist_request(
        "PATCH",
        f"/gists/{STATE_GIST_ID}",
        {
            "files": {
                STATE_GIST_FILENAME: {
                    "content": content
                }
            }
        },
    )

    return True


async def save_state():
    async with state_lock:
        try:
            ok = await asyncio.to_thread(
                save_state_sync
            )

            if ok:
                runtime["last_state_save_at"] = (
                    datetime.now(
                        timezone.utc
                    ).isoformat()
                )

            return ok

        except Exception as exc:
            runtime["last_error"] = (
                f"State save: "
                f"{type(exc).__name__}: {exc}"
            )
            print(
                f"[STATE_SAVE_FAILED] "
                f"{runtime['last_error']}",
                flush=True,
            )
            return False


def apply_restored_state(payload, source):
    one = payload.get("candles_1m") or {}
    five = payload.get("candles_5m") or {}
    fifteen = payload.get("candles_15m") or {}

    stock_one = (
        payload.get("stock_candles_1m")
        or {}
    )
    stock_five = (
        payload.get("stock_candles_5m")
        or {}
    )
    stock_fifteen = (
        payload.get("stock_candles_15m")
        or {}
    )

    loaded = 0

    for symbol in SIGNAL_SYMBOLS:
        candles_1m[symbol].clear()
        candles_5m[symbol].clear()
        candles_15m[symbol].clear()

        for candle in one.get(symbol, [])[-MAX_1M:]:
            if isinstance(candle, dict):
                candles_1m[symbol].append(candle)
                loaded += 1

        for candle in five.get(symbol, [])[-MAX_5M:]:
            if isinstance(candle, dict):
                candles_5m[symbol].append(candle)
                loaded += 1

        for candle in fifteen.get(symbol, [])[-MAX_15M:]:
            if isinstance(candle, dict):
                candles_15m[symbol].append(candle)
                loaded += 1

    for symbol in STOCK_UNIVERSE:
        if isinstance(stock_one.get(symbol), list):
            stock_candles_1m[symbol].clear()
            for candle in stock_one[symbol][-STOCK_MAX_1M:]:
                if isinstance(candle, dict):
                    stock_candles_1m[symbol].append(candle)
                    loaded += 1

        if isinstance(stock_five.get(symbol), list):
            stock_candles_5m[symbol].clear()
            for candle in stock_five[symbol][-STOCK_MAX_5M:]:
                if isinstance(candle, dict):
                    stock_candles_5m[symbol].append(candle)
                    loaded += 1

        if isinstance(stock_fifteen.get(symbol), list):
            stock_candles_15m[symbol].clear()
            for candle in stock_fifteen[symbol][-STOCK_MAX_15M:]:
                if isinstance(candle, dict):
                    stock_candles_15m[symbol].append(candle)
                    loaded += 1

    runtime["state_source"] = source
    runtime["last_state_restore_at"] = (
        datetime.now(
            timezone.utc
        ).isoformat()
    )

    return loaded


def restore_gist_sync():
    if not (
        GITHUB_TOKEN
        and STATE_GIST_ID
    ):
        return 0

    data = gist_request(
        "GET",
        f"/gists/{STATE_GIST_ID}",
    )

    file_info = (
        data.get("files") or {}
    ).get(STATE_GIST_FILENAME) or {}

    content = file_info.get("content")

    if not content:
        return 0

    payload = json.loads(content)

    if not isinstance(payload, dict):
        return 0

    return apply_restored_state(
        payload,
        "github_gist",
    )


def http_json(url):
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent":
                "kingbro-bootstrap-v5"
        },
    )

    with urllib.request.urlopen(
        request,
        timeout=20,
    ) as response:
        return json.loads(
            response.read().decode("utf-8")
        )


def bootstrap_old_service_sync():
    """
    One-time migration helper.

    The uploaded old KING BRO exposes:
      /api/signals/NIFTY50
      /api/signals/SENSEX

    Those responses contain recent 1m/5m/15m candle arrays.
    The old service only needs to stay online until this import succeeds.
    """
    if not BOOTSTRAP_URL:
        return 0

    mapping = {
        "NIFTY 50": "NIFTY50",
        "SENSEX": "SENSEX",
    }

    total = 0

    for symbol, api_symbol in mapping.items():
        try:
            payload = http_json(
                f"{BOOTSTRAP_URL}/api/signals/"
                f"{api_symbol}"
            )

            one = (
                payload.get("one_minute_candles")
                or []
            )
            five = (
                payload.get("five_minute_candles")
                or []
            )
            fifteen = (
                payload.get("fifteen_minute_candles")
                or []
            )

            if one:
                candles_1m[symbol].clear()
                for candle in one[-MAX_1M:]:
                    if isinstance(candle, dict):
                        candles_1m[symbol].append(candle)
                        total += 1

            if five:
                candles_5m[symbol].clear()
                for candle in five[-MAX_5M:]:
                    if isinstance(candle, dict):
                        candles_5m[symbol].append(candle)
                        total += 1

            if fifteen:
                candles_15m[symbol].clear()
                for candle in fifteen[-MAX_15M:]:
                    if isinstance(candle, dict):
                        candles_15m[symbol].append(candle)
                        total += 1

        except Exception as exc:
            print(
                f"[BOOTSTRAP_FAILED] {symbol}: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )

    if total:
        runtime["state_source"] = (
            "old_kingbro_bootstrap"
        )
        runtime["last_state_restore_at"] = (
            datetime.now(
                timezone.utc
            ).isoformat()
        )

    return total


async def restore_state():
    loaded = 0

    try:
        loaded = await asyncio.to_thread(
            restore_gist_sync
        )
    except Exception as exc:
        print(
            f"[GIST_RESTORE_FAILED] "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )

    if loaded == 0 and BOOTSTRAP_URL:
        loaded = await asyncio.to_thread(
            bootstrap_old_service_sync
        )

        if loaded:
            await save_state()

    print(
        f"[STATE_RESTORE] "
        f"loaded={loaded} "
        f"source={runtime.get('state_source')}",
        flush=True,
    )

    return loaded


# =========================================================
# INDICATORS / OLD ORIGINAL INDEX STRATEGY
# =========================================================
def ema(values, period):
    if len(values) < period:
        return None

    k = 2 / (period + 1)
    out = sum(values[:period]) / period

    for value in values[period:]:
        out = value * k + out * (1 - k)

    return out


def sma(values, period):
    if len(values) < period:
        return None

    return sum(values[-period:]) / period


def rsi(values, period=14):
    if len(values) < period + 1:
        return None

    window = values[-(period + 1):]
    gains = []
    losses = []

    for index in range(1, len(window)):
        change = (
            window[index]
            - window[index - 1]
        )

        gains.append(
            max(change, 0)
        )
        losses.append(
            max(-change, 0)
        )

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return (
        100
        - (100 / (1 + rs))
    )


def williams_r(candles, period=14):
    if len(candles) < period:
        return None

    rows = candles[-period:]

    highest = max(
        candle["high"]
        for candle in rows
    )
    lowest = min(
        candle["low"]
        for candle in rows
    )
    close = rows[-1]["close"]

    if highest == lowest:
        return -50.0

    return (
        -100
        * (highest - close)
        / (highest - lowest)
    )


def atr(candles, period=14):
    if len(candles) < period + 1:
        return None

    trs = []

    for index in range(1, len(candles)):
        current = candles[index]
        previous = candles[index - 1]

        tr = max(
            current["high"]
            - current["low"],

            abs(
                current["high"]
                - previous["close"]
            ),

            abs(
                current["low"]
                - previous["close"]
            ),
        )

        trs.append(tr)

    if len(trs) < period:
        return None

    return (
        sum(trs[-period:])
        / period
    )


def price_action(candles):
    if len(candles) < 3:
        return "NEUTRAL"

    a, b, c = (
        candles[-3],
        candles[-2],
        candles[-1],
    )

    if (
        c["high"] > b["high"] > a["high"]
        and
        c["low"] > b["low"] > a["low"]
    ):
        return "BULLISH"

    if (
        c["high"] < b["high"] < a["high"]
        and
        c["low"] < b["low"] < a["low"]
    ):
        return "BEARISH"

    return "NEUTRAL"


def breakout(candles):
    if len(candles) < 2:
        return "NONE"

    previous = candles[-2]
    current = candles[-1]

    if current["close"] > previous["high"]:
        return "BULLISH"

    if current["close"] < previous["low"]:
        return "BEARISH"

    return "NONE"


def session_key_from_candle(candle):
    ts = candle.get("ts")

    if ts is None:
        return None

    try:
        return datetime.fromtimestamp(
            float(ts),
            tz=timezone.utc,
        ).date().isoformat()
    except Exception:
        return None


def previous_session_hlc(candles):
    sessions = {}

    for candle in candles:
        day = session_key_from_candle(
            candle
        )

        if day is None:
            continue

        sessions.setdefault(
            day,
            [],
        ).append(candle)

    if len(sessions) < 2:
        return None

    days = sorted(sessions)
    previous_day = days[-2]
    rows = sessions[previous_day]

    highs = [
        number(c.get("high"))
        for c in rows
    ]
    lows = [
        number(c.get("low"))
        for c in rows
    ]
    closes = [
        number(c.get("close"))
        for c in rows
    ]

    highs = [
        x for x in highs
        if x is not None
    ]
    lows = [
        x for x in lows
        if x is not None
    ]
    closes = [
        x for x in closes
        if x is not None
    ]

    if not highs or not lows or not closes:
        return None

    return {
        "session": previous_day,
        "high": max(highs),
        "low": min(lows),
        "close": closes[-1],
        "candle_count": len(rows),
    }


def pivot_levels(high, low, close):
    h = number(high)
    l = number(low)
    c = number(close)

    if (
        h is None
        or l is None
        or c is None
        or h < l
    ):
        return None

    pivot = (h + l + c) / 3.0
    bc_raw = (h + l) / 2.0
    tc_raw = (2.0 * pivot) - bc_raw
    bottom = min(bc_raw, tc_raw)
    top = max(bc_raw, tc_raw)
    rng = h - l

    return {
        "pivot": round(pivot, 2),

        "cpr": {
            "bc": round(bottom, 2),
            "pivot": round(pivot, 2),
            "tc": round(top, 2),
            "width":
                round(top - bottom, 2),
        },

        "classic": {
            "r1":
                round(
                    (2.0 * pivot) - l,
                    2,
                ),

            "s1":
                round(
                    (2.0 * pivot) - h,
                    2,
                ),
        },

        "fib": {
            "r1":
                round(
                    pivot + 0.382 * rng,
                    2,
                ),

            "r2":
                round(
                    pivot + 0.618 * rng,
                    2,
                ),

            "r3":
                round(
                    pivot + rng,
                    2,
                ),

            "s1":
                round(
                    pivot - 0.382 * rng,
                    2,
                ),

            "s2":
                round(
                    pivot - 0.618 * rng,
                    2,
                ),

            "s3":
                round(
                    pivot - rng,
                    2,
                ),
        },
    }


def refresh_daily_levels(symbol):
    reference = previous_session_hlc(
        list(candles_1m[symbol])
    )

    if reference is None:
        daily_levels[symbol] = {
            "ready": False,
            "reason":
                "Need completed candles spanning two sessions.",
        }
        return daily_levels[symbol]

    levels = pivot_levels(
        reference["high"],
        reference["low"],
        reference["close"],
    )

    if levels is None:
        return daily_levels[symbol]

    daily_levels[symbol] = {
        "ready": True,
        **levels,
        "reference_session":
            reference["session"],
        "source": {
            "high":
                round(
                    reference["high"],
                    2,
                ),

            "low":
                round(
                    reference["low"],
                    2,
                ),

            "close":
                round(
                    reference["close"],
                    2,
                ),

            "candle_count":
                reference["candle_count"],
        },
        "reason": None,
    }

    return daily_levels[symbol]


def five_minute_pivot(five):
    if not five:
        return {
            "ready": False,
            "reason":
                "No completed 5m candle yet.",
        }

    candle = five[-1]

    levels = pivot_levels(
        candle.get("high"),
        candle.get("low"),
        candle.get("close"),
    )

    if levels is None:
        return {
            "ready": False,
            "reason":
                "Last completed 5m candle invalid.",
        }

    return {
        "ready": True,
        **levels,
        "source_candle_ts":
            candle.get("ts"),
    }


def price_vs_levels(price, levels):
    p = number(price)

    if (
        p is None
        or not levels
        or not levels.get("ready")
    ):
        return {
            "ready": False,
            "cpr_position": "UNKNOWN",
            "fib_position": "UNKNOWN",
        }

    cpr = levels["cpr"]
    fib = levels["fib"]

    if p > cpr["tc"]:
        cpr_position = "ABOVE_CPR"
    elif p < cpr["bc"]:
        cpr_position = "BELOW_CPR"
    else:
        cpr_position = "INSIDE_CPR"

    if p >= fib["r1"]:
        fib_position = "ABOVE_R1"
    elif p <= fib["s1"]:
        fib_position = "BELOW_S1"
    else:
        fib_position = "BETWEEN_S1_R1"

    return {
        "ready": True,
        "cpr_position": cpr_position,
        "fib_position": fib_position,
    }


def indicator_snapshot(symbol):
    one = list(candles_1m[symbol])
    five = list(candles_5m[symbol])
    fifteen = list(candles_15m[symbol])

    c1 = [
        c["close"]
        for c in one
    ]
    c5 = [
        c["close"]
        for c in five
    ]
    c15 = [
        c["close"]
        for c in fifteen
    ]

    daily = refresh_daily_levels(
        symbol
    )

    five_pivot = five_minute_pivot(
        five
    )

    current_price = (
        latest.get(
            symbol,
            {},
        ).get("ltp")
    )

    return {
        "one_minute": {
            "count": len(one),
            "ema9": ema(c1, 9),
            "ema21": ema(c1, 21),
            "rsi14": rsi(c1, 14),
            "williams_r14":
                williams_r(one, 14),
            "atr14": atr(one, 14),
            "price_action":
                price_action(one),
            "breakout":
                breakout(one),
        },

        "five_minute": {
            "count": len(five),
            "ema9": ema(c5, 9),
            "ema21": ema(c5, 21),
            "ma20": sma(c5, 20),
            "rsi14": rsi(c5, 14),
            "price_action":
                price_action(five),
        },

        "fifteen_minute": {
            "count": len(fifteen),
            "ema9": ema(c15, 9),
            "ema21": ema(c15, 21),
            "ma20": sma(c15, 20),
            "rsi14": rsi(c15, 14),
            "price_action":
                price_action(fifteen),
            "breakout":
                breakout(fifteen),
        },

        "daily_levels": daily,

        "daily_level_context":
            price_vs_levels(
                current_price,
                daily,
            ),

        "five_minute_levels":
            five_pivot,

        "five_minute_level_context":
            price_vs_levels(
                current_price,
                five_pivot,
            ),
    }


def direction_score(snapshot, direction):
    """
    ORIGINAL V7.3 NIFTY/SENSEX score logic.

    Critical: requirements are NOT reduced.
      22 completed 1m
      21 completed 5m
      9 completed 15m

    The new durable state restores these previous candles next morning,
    so we keep the old strategy without making the user wait 105-135 minutes.
    """
    one = snapshot["one_minute"]
    five = snapshot["five_minute"]
    fifteen = snapshot["fifteen_minute"]

    score = 0
    reasons = []
    blockers = []

    if one["count"] < INDEX_NEED_1M:
        blockers.append(
            f"1m warm-up "
            f"{one['count']}/{INDEX_NEED_1M}"
        )

    if five["count"] < INDEX_NEED_5M:
        blockers.append(
            f"5m warm-up "
            f"{five['count']}/{INDEX_NEED_5M}"
        )

    if fifteen["count"] < INDEX_NEED_15M:
        blockers.append(
            f"15m warm-up "
            f"{fifteen['count']}/{INDEX_NEED_15M}"
        )

    if five["price_action"] == direction:
        score += 20
        reasons.append(
            "5M price action"
        )

    if fifteen["price_action"] == direction:
        score += 10
        reasons.append(
            "15M higher-timeframe price action"
        )

    if (
        fifteen["ema9"] is not None
        and
        fifteen["ema21"] is not None
    ):
        ok = (
            fifteen["ema9"]
            > fifteen["ema21"]
            if direction == "BULLISH"
            else
            fifteen["ema9"]
            < fifteen["ema21"]
        )

        if ok:
            score += 10
            reasons.append(
                "15M EMA 9/21 higher-timeframe confirmation"
            )

    if (
        five["ema9"] is not None
        and
        five["ema21"] is not None
    ):
        ok = (
            five["ema9"]
            > five["ema21"]
            if direction == "BULLISH"
            else
            five["ema9"]
            < five["ema21"]
        )

        if ok:
            score += 15
            reasons.append(
                "5M EMA 9/21"
            )

    if (
        one["ema9"] is not None
        and
        one["ema21"] is not None
    ):
        ok = (
            one["ema9"]
            > one["ema21"]
            if direction == "BULLISH"
            else
            one["ema9"]
            < one["ema21"]
        )

        if ok:
            score += 15
            reasons.append(
                "1M EMA 9/21"
            )

    if one["breakout"] == direction:
        score += 20
        reasons.append(
            "1M breakout"
        )

    value_rsi = one["rsi14"]

    if value_rsi is not None:
        if (
            direction == "BULLISH"
            and 55 <= value_rsi <= 78
        ):
            score += 10
            reasons.append(
                "RSI bullish zone"
            )

        elif (
            direction == "BEARISH"
            and 22 <= value_rsi <= 45
        ):
            score += 10
            reasons.append(
                "RSI bearish zone"
            )

    wr = one["williams_r14"]

    if wr is not None:
        if (
            direction == "BULLISH"
            and -50 <= wr <= -5
        ):
            score += 10
            reasons.append(
                "Williams %R bullish"
            )

        elif (
            direction == "BEARISH"
            and -95 <= wr <= -50
        ):
            score += 10
            reasons.append(
                "Williams %R bearish"
            )

    if one["price_action"] == direction:
        score += 10
        reasons.append(
            "1M price action"
        )

    daily_ctx = (
        snapshot.get(
            "daily_level_context"
        )
        or {}
    )

    if daily_ctx.get("ready"):
        cpr_position = (
            daily_ctx.get(
                "cpr_position"
            )
        )

        fib_position = (
            daily_ctx.get(
                "fib_position"
            )
        )

        if direction == "BULLISH":
            if cpr_position == "ABOVE_CPR":
                score += 5
                reasons.append(
                    "Above daily CPR"
                )

            if fib_position == "ABOVE_R1":
                score += 5
                reasons.append(
                    "Above daily Fib R1"
                )

        else:
            if cpr_position == "BELOW_CPR":
                score += 5
                reasons.append(
                    "Below daily CPR"
                )

            if fib_position == "BELOW_S1":
                score += 5
                reasons.append(
                    "Below daily Fib S1"
                )

    five_ctx = (
        snapshot.get(
            "five_minute_level_context"
        )
        or {}
    )

    if five_ctx.get("ready"):
        cpr_position = (
            five_ctx.get(
                "cpr_position"
            )
        )

        if (
            direction == "BULLISH"
            and
            cpr_position == "ABOVE_CPR"
        ):
            score += 5
            reasons.append(
                "Above 5M pivot CPR"
            )

        elif (
            direction == "BEARISH"
            and
            cpr_position == "BELOW_CPR"
        ):
            score += 5
            reasons.append(
                "Below 5M pivot CPR"
            )

    return (
        min(score, 100),
        reasons,
        blockers,
    )


# =========================================================
# OPTION DISCOVERY / QUOTES — ROBUST CURRENT FIX
# =========================================================
def field(row, *names):
    for name in names:
        if (
            isinstance(row, dict)
            and
            row.get(name)
            not in (None, "")
        ):
            return row.get(name)

    return None


def clean_upper(value):
    return re.sub(
        r"[^A-Z0-9]+",
        "",
        str(value or "").upper(),
    )


def parse_expiry(value):
    raw = str(value or "").strip()

    if not raw:
        return None

    try:
        return date.fromisoformat(
            raw[:10]
        )
    except Exception:
        pass

    compact = re.sub(
        r"[^A-Za-z0-9]",
        "",
        raw,
    ).upper()

    for fmt in (
        "%d%b%Y",
        "%d%b%y",
        "%d%m%Y",
        "%Y%m%d",
    ):
        try:
            return datetime.strptime(
                compact,
                fmt,
            ).date()
        except Exception:
            continue

    return None


def strike_from_trading_symbol(trading_symbol):
    match = re.search(
        r"(\d+(?:\.\d+)?)(CE|PE)$",
        str(
            trading_symbol
            or ""
        ).upper(),
    )

    if not match:
        return None

    try:
        return float(
            match.group(1)
        )
    except Exception:
        return None


def normalise_contract(row):
    token = field(
        row,
        "pSymbol",
        "token",
        "instrument_token",
        "exchange_token",
    )

    expiry = field(
        row,
        "pExpiryDate",
        "expiry",
        "expiry_date",
    )

    option_type = str(
        field(
            row,
            "pOptionType",
            "option_type",
            "optionType",
        )
        or ""
    ).upper().strip()

    trading_symbol = str(
        field(
            row,
            "pTrdSymbol",
            "trading_symbol",
            "display_symbol",
        )
        or ""
    ).strip()

    symbol_name = str(
        field(
            row,
            "pSymbolName",
            "symbol",
            "symbol_name",
        )
        or ""
    ).strip()

    instrument_type = str(
        field(
            row,
            "pInstType",
            "instrument_type",
            "instrumentType",
            "inst_type",
        )
        or ""
    ).upper().strip()

    raw_strike = number(
        field(
            row,
            "dStrikePrice;",
            "dStrikePrice",
            "strike_price",
            "strikePrice",
        )
    )

    parsed_strike = (
        strike_from_trading_symbol(
            trading_symbol
        )
    )

    strike = (
        parsed_strike
        if parsed_strike is not None
        else raw_strike
    )

    return {
        "instrument_token":
            str(token or "").strip(),

        "symbol":
            symbol_name,

        "trading_symbol":
            trading_symbol,

        "expiry":
            str(expiry or "").strip(),

        "expiry_date":
            parse_expiry(expiry),

        "option_type":
            option_type,

        "strike_price":
            strike,

        "instrument_type":
            instrument_type,
    }


def exact_underlying(item, wanted_symbol):
    wanted = clean_upper(
        wanted_symbol
    )

    symbol_name = clean_upper(
        item.get("symbol")
    )

    trading_symbol = clean_upper(
        item.get("trading_symbol")
    )

    if symbol_name:
        return symbol_name == wanted

    if not trading_symbol.startswith(
        wanted
    ):
        return False

    tail = trading_symbol[
        len(wanted):
    ]

    return (
        bool(tail)
        and tail[0].isdigit()
    )


def search_option_sync(
    exchange_segment,
    symbol,
):
    if neo_client is None:
        raise RuntimeError(
            "Kotak login required"
        )

    response = neo_client.search_scrip(
        exchange_segment=
            exchange_segment,

        symbol=
            symbol,

        expiry="",
        option_type="",
        strike_price="",
    )

    if isinstance(response, list):
        return response

    return normalise_rows(
        response
    )


def quote_once_sync(
    instrument_token,
    exchange_segment,
    quote_type,
):
    if neo_client is None:
        raise RuntimeError(
            "Kotak login required"
        )

    response = neo_client.quotes(
        instrument_tokens=[
            {
                "instrument_token":
                    str(instrument_token),

                "exchange_segment":
                    exchange_segment,
            }
        ],
        quote_type=quote_type,
    )

    rows = normalise_rows(
        response
    )

    if rows and isinstance(
        rows[0],
        dict,
    ):
        return rows[0]

    return {}


def merge_quote(base, extra):
    merged = dict(
        base or {}
    )

    if not isinstance(
        extra,
        dict,
    ):
        return merged

    for key, value in extra.items():
        if value in (
            None,
            "",
            [],
            {},
        ):
            continue

        if (
            key == "depth"
            and
            isinstance(
                value,
                dict,
            )
        ):
            depth = dict(
                merged.get("depth")
                or {}
            )

            for side, rows in value.items():
                if rows not in (
                    None,
                    "",
                    [],
                    {},
                ):
                    depth[side] = rows

            merged["depth"] = depth
            continue

        current = merged.get(key)

        if (
            current in (
                None,
                "",
                [],
                {},
            )
            or
            key in {
                "ltp",
                "last_traded_price",
                "lp",
                "open_int",
                "oi",
                "open_interest",
            }
        ):
            merged[key] = value

    return merged


def row_ltp(row):
    return (
        number(row.get("ltp"))
        or
        number(
            row.get(
                "last_traded_price"
            )
        )
        or
        number(row.get("lp"))
    )


def row_oi(row):
    return (
        number(
            row.get(
                "open_int"
            )
        )
        or
        number(row.get("oi"))
        or
        number(
            row.get(
                "open_interest"
            )
        )
    )


def quote_with_fallback_sync(
    instrument_token,
    exchange_segment,
):
    """
    Current robust fix retained from uploaded V7.6/V7.7:
      all -> ltp -> oi -> market_depth -> depth
    """
    merged = {}

    try:
        merged = merge_quote(
            merged,
            quote_once_sync(
                instrument_token,
                exchange_segment,
                "all",
            ),
        )
    except Exception:
        pass

    if not row_ltp(merged):
        try:
            merged = merge_quote(
                merged,
                quote_once_sync(
                    instrument_token,
                    exchange_segment,
                    "ltp",
                ),
            )
        except Exception:
            pass

    if (
        exchange_segment.lower().endswith("_fo")
        and
        not row_oi(merged)
    ):
        try:
            merged = merge_quote(
                merged,
                quote_once_sync(
                    instrument_token,
                    exchange_segment,
                    "oi",
                ),
            )
        except Exception:
            pass

    depth = (
        merged.get("depth")
        or {}
    )

    if not (
        depth.get("buy")
        and
        depth.get("sell")
    ):
        for quote_type in (
            "market_depth",
            "depth",
        ):
            try:
                merged = merge_quote(
                    merged,
                    quote_once_sync(
                        instrument_token,
                        exchange_segment,
                        quote_type,
                    ),
                )
            except Exception:
                pass

            depth = (
                merged.get("depth")
                or {}
            )

            if (
                depth.get("buy")
                and
                depth.get("sell")
            ):
                break

    return merged


def depth_totals(rows):
    quantity = 0.0

    if not isinstance(
        rows,
        list,
    ):
        return quantity

    for row in rows:
        if not isinstance(
            row,
            dict,
        ):
            continue

        quantity += (
            number(
                row.get("quantity")
            )
            or 0.0
        )

    return quantity


def best_depth_price(rows):
    if not isinstance(
        rows,
        list,
    ) or not rows:
        return None

    first = rows[0]

    if not isinstance(
        first,
        dict,
    ):
        return None

    return number(
        first.get("price")
    )


def analyse_option_quote(row):
    depth = (
        row.get("depth")
        or {}
    )

    buys = (
        depth.get("buy")
        or []
    )

    sells = (
        depth.get("sell")
        or []
    )

    buy_qty = depth_totals(
        buys
    )

    sell_qty = depth_totals(
        sells
    )

    best_bid = best_depth_price(
        buys
    )

    best_ask = best_depth_price(
        sells
    )

    spread_pct = None

    if (
        best_bid is not None
        and
        best_ask is not None
        and
        best_ask >= best_bid
    ):
        mid = (
            best_bid + best_ask
        ) / 2.0

        if mid > 0:
            spread_pct = (
                (best_ask - best_bid)
                / mid
            ) * 100.0

    ltp = row_ltp(row)
    oi = row_oi(row)

    volume = (
        number(
            row.get(
                "last_volume"
            )
        )
        or
        number(
            row.get("volume")
        )
        or
        number(
            row.get("vol")
        )
    )

    liquidity_score = 0
    liquidity_reasons = []

    if ltp is not None and ltp > 0:
        liquidity_score += 20
        liquidity_reasons.append(
            "valid LTP"
        )

    if volume is not None and volume > 0:
        liquidity_score += 20
        liquidity_reasons.append(
            "traded volume"
        )

    if oi is not None and oi > 0:
        liquidity_score += 20
        liquidity_reasons.append(
            "open interest"
        )

    if (
        best_bid is not None
        and
        best_ask is not None
    ):
        liquidity_score += 20
        liquidity_reasons.append(
            "two-sided market"
        )

    if spread_pct is not None:
        if spread_pct <= 1.0:
            liquidity_score += 20
            liquidity_reasons.append(
                "tight spread"
            )

        elif spread_pct <= 2.0:
            liquidity_score += 10
            liquidity_reasons.append(
                "acceptable spread"
            )

    total_depth = (
        buy_qty + sell_qty
    )

    depth_imbalance = None

    if total_depth > 0:
        depth_imbalance = (
            (buy_qty - sell_qty)
            / total_depth
        ) * 100.0

    return {
        "ltp": ltp,
        "open_interest": oi,
        "volume": volume,
        "liquidity_score":
            liquidity_score,
        "liquidity_reasons":
            liquidity_reasons,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread_pct": spread_pct,
        "depth_imbalance":
            depth_imbalance,
    }


def atm_candidates(
    rows,
    underlying_ltp,
    direction,
    wanted_symbol,
):
    wanted_type = (
        "CE"
        if direction == "CALL"
        else "PE"
    )

    today = (
        datetime.now(
            timezone.utc
        ).date()
    )

    contracts = []

    for row in rows:
        if not isinstance(
            row,
            dict,
        ):
            continue

        item = normalise_contract(
            row
        )

        if not exact_underlying(
            item,
            wanted_symbol,
        ):
            continue

        instrument_type = clean_upper(
            item.get(
                "instrument_type"
            )
        )

        if (
            instrument_type
            and
            instrument_type
            not in {
                "OPTIDX",
                "IO",
            }
        ):
            continue

        if (
            item["option_type"]
            != wanted_type
        ):
            continue

        if not item[
            "instrument_token"
        ]:
            continue

        if (
            item["strike_price"]
            is None
        ):
            continue

        expiry_date = (
            item.get(
                "expiry_date"
            )
        )

        if (
            expiry_date is None
            or
            expiry_date < today
        ):
            continue

        contracts.append(
            item
        )

    if not contracts:
        return []

    nearest_expiry = min(
        item["expiry_date"]
        for item in contracts
    )

    contracts = [
        item
        for item in contracts
        if item["expiry_date"]
        == nearest_expiry
    ]

    contracts.sort(
        key=lambda item:
            abs(
                float(
                    item[
                        "strike_price"
                    ]
                )
                -
                float(
                    underlying_ltp
                )
            )
    )

    return contracts


async def auto_discover_option(
    symbol,
    direction,
):
    ltp = (
        latest.get(
            symbol,
            {},
        ).get("ltp")
    )

    if ltp is None:
        raise RuntimeError(
            f"No underlying LTP "
            f"for {symbol}"
        )

    if symbol == "NIFTY 50":
        exchange_segment = "nse_fo"
        search_symbol = "NIFTY"

    elif symbol == "SENSEX":
        exchange_segment = "bse_fo"
        search_symbol = "SENSEX"

    else:
        raise RuntimeError(
            "Unsupported underlying"
        )

    rows = await asyncio.to_thread(
        search_option_sync,
        exchange_segment,
        search_symbol,
    )

    candidates = atm_candidates(
        rows,
        float(ltp),
        direction,
        search_symbol,
    )

    if not candidates:
        return {
            "ready": False,
            "reason":
                "No exact current/future nearest-expiry option candidates.",
        }

    # Check 8 nearest-expiry ATM-neighbourhood contracts.
    inspected = []

    for candidate in candidates[:8]:
        try:
            quote_row = (
                await asyncio.to_thread(
                    quote_with_fallback_sync,
                    candidate[
                        "instrument_token"
                    ],
                    exchange_segment,
                )
            )

            analysis = (
                analyse_option_quote(
                    quote_row
                )
            )

            inspected.append({
                **candidate,
                "quote": analysis,
            })

        except Exception as exc:
            inspected.append({
                **candidate,
                "quote_error":
                    f"{type(exc).__name__}: {exc}",
            })

    usable = [
        item
        for item in inspected
        if isinstance(
            item.get("quote"),
            dict,
        )
        and
        item["quote"].get(
            "ltp"
        ) is not None
        and
        float(
            item["quote"]["ltp"]
        ) > 0
    ]

    if not usable:
        return {
            "ready": False,
            "reason":
                "Nearest-expiry candidates found but no usable live option premium after all→ltp fallback.",
            "candidates_checked":
                inspected,
        }

    usable.sort(
        key=lambda item: (
            abs(
                float(
                    item[
                        "strike_price"
                    ]
                )
                -
                float(ltp)
            ),
            -(
                item[
                    "quote"
                ].get(
                    "liquidity_score"
                )
                or 0
            ),
        )
    )

    selected = usable[0]

    return {
        "ready": True,
        "exchange_segment":
            exchange_segment,
        "selected":
            selected,
        "candidates_checked":
            inspected,
    }


def option_trade_plan(option_ltp):
    """
    EXACT OLD screenshot/trade-plan model:
      15% premium stop
      T1 = 1R
      T2 = 2R

    This matches the earlier alerts such as:
      Entry 408.65
      SL 347.35
      T1 469.95
      T2 531.25
    """
    if (
        option_ltp is None
        or
        option_ltp <= 0
    ):
        return None

    def tick(value):
        return round(
            round(
                float(value)
                / 0.05
            ) * 0.05,
            2,
        )

    entry = tick(
        option_ltp
    )

    stop = tick(
        option_ltp * 0.85
    )

    risk = max(
        entry - stop,
        0.05,
    )

    return {
        "entry":
            entry,

        "stop_loss":
            stop,

        "target_1":
            tick(
                entry + risk
            ),

        "target_2":
            tick(
                entry + 2 * risk
            ),

        "rr_target_1":
            "1:1",

        "rr_target_2":
            "1:2",
    }


# =========================================================
# INDEX SIGNAL EVALUATION — ORIGINAL WINNING SCORE ENGINE
# =========================================================
def grade_for_score(score):
    if score >= A_PLUS_SCORE:
        return "A+"

    if score >= STRONG_SCORE:
        return "STRONG"

    if score >= WATCH_SCORE:
        return "WATCH"

    return "NO_TRADE"


async def evaluate_index_signal(symbol):
    if (
        not runtime[
            "index_scan_enabled"
        ]
        or
        not signal_window_open()
    ):
        return

    snapshot = indicator_snapshot(
        symbol
    )

    bull_score, bull_reasons, bull_blockers = (
        direction_score(
            snapshot,
            "BULLISH",
        )
    )

    bear_score, bear_reasons, bear_blockers = (
        direction_score(
            snapshot,
            "BEARISH",
        )
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

    grade = grade_for_score(
        score
    )

    if blockers:
        runtime["last_index_blocker"] = (
            f"{symbol}: "
            + ", ".join(blockers)
        )
        print(
            f"[INDEX_WARMUP] "
            f"{runtime['last_index_blocker']}",
            flush=True,
        )
        return

    if grade not in {
        "A+",
        "STRONG",
    }:
        print(
            f"[INDEX_NO_TRADE] "
            f"{symbol} {direction} "
            f"score={score} grade={grade}",
            flush=True,
        )
        return

    signal_key = (
        f"INDEX|{symbol}|{direction}"
    )

    now_ts = (
        datetime.now(
            timezone.utc
        ).timestamp()
    )

    previous = (
        index_alert_cache.get(
            signal_key
        )
    )

    if (
        previous
        and
        now_ts - previous
        < ALERT_COOLDOWN_SECONDS
    ):
        return

    # ORIGINAL strategy required real option confirmation.
    # Current robust quote fallback is used before applying the same quality gate.
    try:
        discovery = await asyncio.wait_for(
            auto_discover_option(
                symbol,
                direction,
            ),
            timeout=30,
        )

    except Exception as exc:
        runtime["last_index_blocker"] = (
            f"{symbol}: option discovery error: "
            f"{type(exc).__name__}: {exc}"
        )
        return

    if not discovery.get("ready"):
        runtime["last_index_blocker"] = (
            f"{symbol}: "
            f"{discovery.get('reason')}"
        )
        print(
            f"[OPTION_NOT_READY] "
            f"{runtime['last_index_blocker']}",
            flush=True,
        )
        return

    selected = discovery[
        "selected"
    ]

    quote = (
        selected.get("quote")
        or {}
    )

    option_ltp = quote.get(
        "ltp"
    )

    oi = quote.get(
        "open_interest"
    )

    liquidity_score = (
        quote.get(
            "liquidity_score"
        )
        or 0
    )

    quality_pass = (
        option_ltp is not None
        and
        option_ltp > 0
        and
        oi is not None
        and
        oi > 0
        and
        liquidity_score
        >= OPTION_MIN_LIQUIDITY
    )

    if not quality_pass:
        runtime["last_index_blocker"] = (
            f"{symbol}: option filter "
            f"LTP={option_ltp} "
            f"OI={oi} "
            f"liquidity={liquidity_score}/100"
        )
        print(
            f"[OPTION_FILTER] "
            f"{runtime['last_index_blocker']}",
            flush=True,
        )
        return

    plan = option_trade_plan(
        float(option_ltp)
    )

    if plan is None:
        return

    contract_name = (
        selected.get(
            "trading_symbol"
        )
        or "-"
    )

    expiry = (
        selected.get("expiry")
        or "-"
    )

    lines = [
        "👑 THE RAAJA BRO — SIGNAL",
        "",
        f"📊 {symbol}",
        f"🎯 {direction} | {grade}",
        f"⭐ Score: {score}/100",
        f"🧾 Option: {contract_name}",
        f"Index/Spot: "
        f"{latest.get(symbol, {}).get('ltp')}",
        f"Option LTP: ₹{float(option_ltp):.2f}",
        f"Entry: ₹{plan['entry']:.2f}",
        f"SL: ₹{plan['stop_loss']:.2f}",
        f"T1: ₹{plan['target_1']:.2f}",
        f"T2: ₹{plan['target_2']:.2f}",
        f"Expiry: {expiry}",
        f"OI: {oi}",
        f"Option liquidity: "
        f"{liquidity_score}/100",
        "",
        "Confirmations:",
    ]

    lines.extend(
        f"• {reason}"
        for reason in reasons[:10]
    )

    lines.extend([
        "",
        "⚠️ MANUAL ORDER ONLY — no automatic execution.",
        f"Time: "
        f"{ist_now().strftime('%Y-%m-%d %I:%M:%S %p IST')}",
    ])

    ok = await telegram_send(
        "\n".join(lines)
    )

    if ok:
        index_alert_cache[
            signal_key
        ] = now_ts

        runtime[
            "last_index_signal_at"
        ] = (
            datetime.now(
                timezone.utc
            ).isoformat()
        )

        runtime[
            "last_index_blocker"
        ] = None

        print(
            f"[INDEX_SIGNAL_SENT] "
            f"{symbol} {direction} "
            f"score={score} "
            f"premium={option_ltp}",
            flush=True,
        )


# =========================================================
# ORIGINAL STOCK STRATEGY
# =========================================================
def stock_snapshot(symbol):
    one = list(
        stock_candles_1m[
            symbol
        ]
    )

    five = list(
        stock_candles_5m[
            symbol
        ]
    )

    fifteen = list(
        stock_candles_15m[
            symbol
        ]
    )

    c1 = [
        c["close"]
        for c in one
    ]

    c5 = [
        c["close"]
        for c in five
    ]

    c15 = [
        c["close"]
        for c in fifteen
    ]

    return {
        "one": {
            "count": len(one),
            "ema9": ema(c1, 9),
            "ema21": ema(c1, 21),
            "rsi14": rsi(c1, 14),
            "williams_r14":
                williams_r(
                    one,
                    14,
                ),
            "price_action":
                price_action(one),
            "breakout":
                breakout(one),
            "atr14":
                atr(one, 14),
        },

        "five": {
            "count": len(five),
            "ema9": ema(c5, 9),
            "ema21": ema(c5, 21),
            "ma20": sma(c5, 20),
            "rsi14": rsi(c5, 14),
            "price_action":
                price_action(five),
            "breakout":
                breakout(five),
        },

        "fifteen": {
            "count": len(fifteen),
            "ema9": ema(c15, 9),
            "ema21": ema(c15, 21),
            "rsi14": rsi(c15, 14),
            "price_action":
                price_action(fifteen),
            "breakout":
                breakout(fifteen),
        },
    }


def score_stock_direction(
    snapshot,
    direction,
):
    one = snapshot["one"]
    five = snapshot["five"]
    fifteen = snapshot["fifteen"]

    score = 0
    reasons = []
    blockers = []

    if one["count"] < STOCK_NEED_1M:
        blockers.append(
            f"1m warm-up "
            f"{one['count']}/{STOCK_NEED_1M}"
        )

    if five["count"] < STOCK_NEED_5M:
        blockers.append(
            f"5m warm-up "
            f"{five['count']}/{STOCK_NEED_5M}"
        )

    if fifteen["count"] < STOCK_NEED_15M:
        blockers.append(
            f"15m warm-up "
            f"{fifteen['count']}/{STOCK_NEED_15M}"
        )

    bullish = (
        direction == "BUY"
    )

    if (
        fifteen["price_action"]
        ==
        (
            "BULLISH"
            if bullish
            else "BEARISH"
        )
    ):
        score += 20
        reasons.append(
            "15M price action"
        )

    if (
        five["ema9"] is not None
        and
        five["ema21"] is not None
    ):
        ok = (
            five["ema9"]
            > five["ema21"]
            if bullish
            else
            five["ema9"]
            < five["ema21"]
        )

        if ok:
            score += 20
            reasons.append(
                "5M EMA 9/21"
            )

    if (
        one["ema9"] is not None
        and
        one["ema21"] is not None
    ):
        ok = (
            one["ema9"]
            > one["ema21"]
            if bullish
            else
            one["ema9"]
            < one["ema21"]
        )

        if ok:
            score += 15
            reasons.append(
                "1M EMA 9/21"
            )

    if (
        one["breakout"]
        ==
        (
            "BULLISH"
            if bullish
            else "BEARISH"
        )
    ):
        score += 20
        reasons.append(
            "1M breakout"
        )

    value_rsi = one["rsi14"]

    if value_rsi is not None:
        if (
            bullish
            and
            54 <= value_rsi <= 75
        ):
            score += 10
            reasons.append(
                "RSI bullish"
            )

        elif (
            not bullish
            and
            25 <= value_rsi <= 46
        ):
            score += 10
            reasons.append(
                "RSI bearish"
            )

    wr = one["williams_r14"]

    if wr is not None:
        if (
            bullish
            and
            -55 <= wr <= -5
        ):
            score += 10
            reasons.append(
                "Williams %R bullish"
            )

        elif (
            not bullish
            and
            -95 <= wr <= -45
        ):
            score += 10
            reasons.append(
                "Williams %R bearish"
            )

    if (
        five["price_action"]
        ==
        (
            "BULLISH"
            if bullish
            else "BEARISH"
        )
    ):
        score += 5
        reasons.append(
            "5M price action"
        )

    return (
        min(score, 100),
        reasons,
        blockers,
    )


async def evaluate_stock_signal(symbol):
    if (
        not runtime[
            "stock_scan_enabled"
        ]
        or
        not signal_window_open()
    ):
        return

    snapshot = stock_snapshot(
        symbol
    )

    buy_score, buy_reasons, buy_blockers = (
        score_stock_direction(
            snapshot,
            "BUY",
        )
    )

    sell_score, sell_reasons, sell_blockers = (
        score_stock_direction(
            snapshot,
            "SELL",
        )
    )

    if buy_score >= sell_score:
        direction = "BUY"
        score = buy_score
        reasons = buy_reasons
        blockers = buy_blockers

    else:
        direction = "SELL"
        score = sell_score
        reasons = sell_reasons
        blockers = sell_blockers

    if blockers:
        return

    if score >= A_PLUS_SCORE:
        grade = "A+"

    elif score >= STRONG_SCORE:
        grade = "STRONG"

    else:
        return

    ltp = (
        stock_latest.get(
            symbol,
            {},
        ).get("ltp")
    )

    value_atr = (
        snapshot[
            "one"
        ].get("atr14")
    )

    if (
        ltp is None
        or
        value_atr is None
        or
        value_atr <= 0
    ):
        return

    key = (
        f"STOCK|"
        f"{symbol}|"
        f"{direction}"
    )

    now_ts = (
        datetime.now(
            timezone.utc
        ).timestamp()
    )

    previous = (
        stock_alert_cache.get(
            key
        )
    )

    if (
        previous
        and
        now_ts - previous
        < ALERT_COOLDOWN_SECONDS
    ):
        return

    entry = round(
        float(ltp),
        2,
    )

    risk = max(
        float(value_atr) * 1.2,
        entry * 0.003,
    )

    if direction == "BUY":
        stop = round(
            entry - risk,
            2,
        )
        target_1 = round(
            entry + risk,
            2,
        )
        target_2 = round(
            entry + 2 * risk,
            2,
        )

    else:
        stop = round(
            entry + risk,
            2,
        )
        target_1 = round(
            entry - risk,
            2,
        )
        target_2 = round(
            entry - 2 * risk,
            2,
        )

    lines = [
        "👑 THE RAAJA BRO — STOCK SIGNAL",
        "",
        f"📊 {symbol}",
        f"🎯 {direction} | {grade}",
        f"⭐ Score: {score}/100",
        f"Entry: ₹{entry:.2f}",
        f"SL: ₹{stop:.2f}",
        f"T1: ₹{target_1:.2f}",
        f"T2: ₹{target_2:.2f}",
        "",
        "Confirmations:",
    ]

    lines.extend(
        f"• {reason}"
        for reason in reasons[:8]
    )

    lines.extend([
        "",
        "⚠️ MANUAL ORDER ONLY — no automatic execution.",
        f"Time: "
        f"{ist_now().strftime('%Y-%m-%d %I:%M:%S %p IST')}",
    ])

    ok = await telegram_send(
        "\n".join(lines)
    )

    if ok:
        stock_alert_cache[
            key
        ] = now_ts

        runtime[
            "last_stock_signal_at"
        ] = (
            datetime.now(
                timezone.utc
            ).isoformat()
        )


# =========================================================
# CANDLE CONSUMERS
# =========================================================
async def consume_index_tick(
    symbol,
    price,
    received_at,
):
    try:
        epoch = int(
            datetime.fromisoformat(
                received_at.replace(
                    "Z",
                    "+00:00",
                )
            ).timestamp()
        )

    except Exception:
        epoch = int(
            datetime.now(
                timezone.utc
            ).timestamp()
        )

    closed_any = False
    closed_5m = False

    for (
        minutes,
        active,
        history,
    ) in (
        (
            1,
            active_1m,
            candles_1m,
        ),
        (
            5,
            active_5m,
            candles_5m,
        ),
        (
            15,
            active_15m,
            candles_15m,
        ),
    ):
        bucket = bucket_start(
            epoch,
            minutes,
        )

        current = active[
            symbol
        ]

        if current is None:
            active[symbol] = (
                new_candle(
                    bucket,
                    price,
                )
            )

        elif int(
            current["ts"]
        ) == bucket:
            update_candle(
                current,
                price,
            )

        elif bucket > int(
            current["ts"]
        ):
            history[symbol].append(
                dict(current)
            )

            active[symbol] = (
                new_candle(
                    bucket,
                    price,
                )
            )

            closed_any = True

            if minutes == 5:
                closed_5m = True

    # Evaluate on every completed candle boundary.
    # With previous state restored, this can work from 09:30 onward.
    if closed_any:
        await evaluate_index_signal(
            symbol
        )

    # Persist on 5-minute boundaries to keep GitHub writes modest.
    if closed_5m:
        await save_state()


async def consume_stock_tick(
    symbol,
    price,
    received_at,
):
    if not runtime[
        "stock_scan_enabled"
    ]:
        return

    try:
        epoch = int(
            datetime.fromisoformat(
                received_at.replace(
                    "Z",
                    "+00:00",
                )
            ).timestamp()
        )
    except Exception:
        epoch = int(
            datetime.now(
                timezone.utc
            ).timestamp()
        )

    closed_any = False
    closed_5m = False

    for (
        minutes,
        active,
        history,
    ) in (
        (
            1,
            stock_active_1m,
            stock_candles_1m,
        ),
        (
            5,
            stock_active_5m,
            stock_candles_5m,
        ),
        (
            15,
            stock_active_15m,
            stock_candles_15m,
        ),
    ):
        bucket = bucket_start(
            epoch,
            minutes,
        )

        current = active[
            symbol
        ]

        if current is None:
            active[symbol] = (
                new_candle(
                    bucket,
                    price,
                )
            )

        elif int(
            current["ts"]
        ) == bucket:
            update_candle(
                current,
                price,
            )

        elif bucket > int(
            current["ts"]
        ):
            history[symbol].append(
                dict(current)
            )

            active[symbol] = (
                new_candle(
                    bucket,
                    price,
                )
            )

            closed_any = True

            if minutes == 5:
                closed_5m = True

    if closed_any:
        await evaluate_stock_signal(
            symbol
        )

    if closed_5m:
        await save_state()


# =========================================================
# AUTH
# =========================================================
def authenticate_sync(totp):
    required = {
        "KOTAK_CONSUMER_KEY":
            KOTAK_CONSUMER_KEY,

        "KOTAK_MOBILE_NUMBER":
            KOTAK_MOBILE_NUMBER,

        "KOTAK_UCC":
            KOTAK_UCC,

        "KOTAK_MPIN":
            KOTAK_MPIN,
    }

    missing = [
        key
        for key, value
        in required.items()
        if not value
    ]

    if missing:
        raise RuntimeError(
            "Missing env: "
            + ", ".join(missing)
        )

    client = NeoAPI(
        consumer_key=
            KOTAK_CONSUMER_KEY,

        environment=
            KOTAK_ENVIRONMENT,
    )

    login_response = (
        client.totp_login(
            mobile_number=
                KOTAK_MOBILE_NUMBER,

            ucc=
                KOTAK_UCC,

            totp=
                totp,
        )
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

    validate_response = (
        client.totp_validate(
            mpin=
                KOTAK_MPIN
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


async def login_with_totp(totp):
    global neo_client
    global index_feed_task
    global stock_feed_task

    if (
        not totp.isdigit()
        or len(totp) != 6
    ):
        raise RuntimeError(
            "TOTP must be exactly 6 digits"
        )

    client = (
        await asyncio.to_thread(
            authenticate_sync,
            totp,
        )
    )

    neo_client = client

    runtime[
        "broker_connected"
    ] = True

    runtime[
        "index_feed_connected"
    ] = False

    runtime[
        "last_login_at"
    ] = (
        datetime.now(
            timezone.utc
        ).isoformat()
    )

    runtime[
        "last_error"
    ] = None

    if (
        index_feed_task
        and
        not index_feed_task.done()
    ):
        index_feed_task.cancel()

    index_feed_task = (
        asyncio.create_task(
            index_feed_loop()
        )
    )

    # If user already enabled STOCK scan, restart it after re-login.
    if runtime[
        "stock_scan_enabled"
    ]:
        if (
            stock_feed_task
            and
            not stock_feed_task.done()
        ):
            stock_feed_task.cancel()

        stock_feed_task = (
            asyncio.create_task(
                stock_feed_loop()
            )
        )

    return True


# =========================================================
# INDEX LIVE FEED
# =========================================================
async def index_feed_loop():
    global neo_client

    backoff = 2

    while neo_client is not None:
        try:
            async with (
                neo_client
                .create_websocket()
            ) as websocket:

                await websocket.subscribe_index([
                    WsToken(
                        "nse_cm",
                        "Nifty 50",
                    ),

                    WsToken(
                        "bse_cm",
                        "SENSEX",
                    ),
                ])

                runtime[
                    "index_feed_connected"
                ] = True

                runtime[
                    "last_error"
                ] = None

                backoff = 2

                print(
                    "[INDEX_FEED_CONNECTED]",
                    flush=True,
                )

                async for message in websocket:
                    if not isinstance(
                        message,
                        SFeedIndex,
                    ):
                        continue

                    try:
                        data = (
                            message.model_dump()
                        )
                    except Exception:
                        data = {}

                    instrument_token = str(
                        data.get(
                            "instrument_token"
                        )
                        or ""
                    )

                    trading_symbol = str(
                        data.get(
                            "trading_symbol"
                        )
                        or
                        data.get(
                            "display_symbol"
                        )
                        or
                        data.get(
                            "index_name"
                        )
                        or ""
                    )

                    symbol = (
                        canonical_index_name(
                            instrument_token,
                            trading_symbol,
                        )
                    )

                    if symbol not in SIGNAL_SYMBOLS:
                        continue

                    raw_ltp = (
                        data.get(
                            "last_traded_price"
                        )
                        or
                        data.get("ltp")
                        or
                        data.get(
                            "index_value"
                        )
                        or
                        data.get(
                            "last_price"
                        )
                    )

                    value_ltp = number(
                        raw_ltp
                    )

                    if value_ltp is None:
                        continue

                    now = (
                        datetime.now(
                            timezone.utc
                        ).isoformat()
                    )

                    latest[symbol] = {
                        "ltp":
                            value_ltp,

                        "received_at":
                            now,
                    }

                    runtime[
                        "last_tick_at"
                    ] = now

                    await consume_index_tick(
                        symbol,
                        value_ltp,
                        now,
                    )

            runtime[
                "index_feed_connected"
            ] = False

            await asyncio.sleep(
                backoff
            )

            backoff = min(
                backoff * 2,
                30,
            )

        except asyncio.CancelledError:
            runtime[
                "index_feed_connected"
            ] = False
            return

        except Exception as exc:
            runtime[
                "index_feed_connected"
            ] = False

            runtime[
                "last_error"
            ] = (
                f"Index feed: "
                f"{type(exc).__name__}: {exc}"
            )

            print(
                f"[INDEX_FEED_ERROR] "
                f"{runtime['last_error']}",
                flush=True,
            )

            await asyncio.sleep(
                backoff
            )

            backoff = min(
                backoff * 2,
                30,
            )


# =========================================================
# STOCK LIVE FEED
# =========================================================
def resolve_stock_sync(symbol):
    if neo_client is None:
        raise RuntimeError(
            "Kotak login required"
        )

    response = neo_client.search_scrip(
        exchange_segment="nse_cm",
        symbol=symbol,
        expiry="",
        option_type="",
        strike_price="",
    )

    rows = normalise_rows(
        response
    )

    wanted = str(
        symbol
    ).upper().strip()

    for row in rows:
        if not isinstance(
            row,
            dict,
        ):
            continue

        trading_symbol = str(
            row.get(
                "pTrdSymbol"
            )
            or
            row.get(
                "trading_symbol"
            )
            or
            row.get(
                "display_symbol"
            )
            or ""
        ).upper().strip()

        symbol_name = str(
            row.get(
                "pSymbolName"
            )
            or
            row.get("symbol")
            or
            row.get(
                "symbol_name"
            )
            or ""
        ).upper().strip()

        token = (
            row.get("pSymbol")
            or
            row.get(
                "instrument_token"
            )
            or
            row.get(
                "exchange_token"
            )
        )

        instrument_type = str(
            row.get(
                "pInstType"
            )
            or
            row.get(
                "instrument_type"
            )
            or ""
        ).upper().strip()

        exact_eq = (
            trading_symbol
            == f"{wanted}-EQ"
        )

        exact_name = (
            symbol_name
            == wanted
        )

        if (
            token not in (
                None,
                "",
            )
            and
            (
                exact_eq
                or exact_name
            )
            and
            instrument_type
            not in {
                "OPTIDX",
                "FUTIDX",
                "OPTSTK",
                "FUTSTK",
            }
        ):
            return {
                "symbol":
                    wanted,

                "instrument_token":
                    str(token),

                "exchange_segment":
                    "nse_cm",

                "trading_symbol":
                    trading_symbol
                    or
                    f"{wanted}-EQ",
            }

    return None


async def resolve_stock_universe():
    resolved = {}
    unresolved = []

    for symbol in STOCK_UNIVERSE:
        try:
            item = (
                await asyncio.to_thread(
                    resolve_stock_sync,
                    symbol,
                )
            )

            if item:
                resolved[symbol] = item
            else:
                unresolved.append(
                    symbol
                )

        except Exception:
            unresolved.append(
                symbol
            )

    stock_token_map.clear()
    stock_token_map.update(
        resolved
    )

    runtime[
        "stock_resolved"
    ] = len(resolved)

    runtime[
        "stock_unresolved"
    ] = len(unresolved)

    return (
        resolved,
        unresolved,
    )


def stock_symbol_from_message(data):
    token = str(
        data.get(
            "instrument_token"
        )
        or
        data.get(
            "exchange_token"
        )
        or ""
    )

    trading_symbol = str(
        data.get(
            "trading_symbol"
        )
        or
        data.get(
            "display_symbol"
        )
        or ""
    ).upper()

    for (
        symbol,
        item,
    ) in stock_token_map.items():

        if (
            token
            and
            token
            ==
            item.get(
                "instrument_token"
            )
        ):
            return symbol

        expected = str(
            item.get(
                "trading_symbol"
            )
            or ""
        ).upper()

        if (
            trading_symbol
            and
            expected
            and
            trading_symbol == expected
        ):
            return symbol

    return None


async def stock_feed_loop():
    global neo_client

    backoff = 2

    while (
        runtime[
            "stock_scan_enabled"
        ]
        and
        neo_client is not None
    ):
        try:
            (
                resolved,
                unresolved,
            ) = await resolve_stock_universe()

            if not resolved:
                raise RuntimeError(
                    "No stock tokens resolved"
                )

            tokens = [
                WsToken(
                    item[
                        "exchange_segment"
                    ],
                    item[
                        "instrument_token"
                    ],
                )
                for item
                in resolved.values()
            ]

            async with (
                neo_client
                .create_websocket()
            ) as websocket:

                await websocket.subscribe_scrips(
                    tokens
                )

                runtime[
                    "stock_feed_connected"
                ] = True

                runtime[
                    "stock_error"
                ] = None

                backoff = 2

                print(
                    f"[STOCK_FEED_CONNECTED] "
                    f"resolved={len(resolved)} "
                    f"unresolved={len(unresolved)}",
                    flush=True,
                )

                async for message in websocket:
                    if not runtime[
                        "stock_scan_enabled"
                    ]:
                        break

                    if not isinstance(
                        message,
                        SFeedScrip,
                    ):
                        continue

                    try:
                        data = (
                            message.model_dump()
                        )
                    except Exception:
                        data = {}

                    symbol = (
                        stock_symbol_from_message(
                            data
                        )
                    )

                    if symbol is None:
                        continue

                    raw_ltp = (
                        data.get(
                            "last_traded_price"
                        )
                        or
                        data.get("ltp")
                        or
                        data.get(
                            "last_price"
                        )
                    )

                    value_ltp = number(
                        raw_ltp
                    )

                    if (
                        value_ltp is None
                        or value_ltp <= 0
                    ):
                        continue

                    now = (
                        datetime.now(
                            timezone.utc
                        ).isoformat()
                    )

                    stock_latest[
                        symbol
                    ] = {
                        "ltp":
                            value_ltp,

                        "received_at":
                            now,
                    }

                    await consume_stock_tick(
                        symbol,
                        value_ltp,
                        now,
                    )

            runtime[
                "stock_feed_connected"
            ] = False

            if runtime[
                "stock_scan_enabled"
            ]:
                await asyncio.sleep(
                    backoff
                )

                backoff = min(
                    backoff * 2,
                    30,
                )

        except asyncio.CancelledError:
            runtime[
                "stock_feed_connected"
            ] = False
            return

        except Exception as exc:
            runtime[
                "stock_feed_connected"
            ] = False

            runtime[
                "stock_error"
            ] = (
                f"{type(exc).__name__}: {exc}"
            )

            print(
                f"[STOCK_FEED_ERROR] "
                f"{runtime['stock_error']}",
                flush=True,
            )

            if runtime[
                "stock_scan_enabled"
            ]:
                await asyncio.sleep(
                    backoff
                )

                backoff = min(
                    backoff * 2,
                    30,
                )


async def start_stock_scan():
    global stock_feed_task

    if (
        neo_client is None
        or
        not runtime[
            "broker_connected"
        ]
    ):
        raise RuntimeError(
            "Login to Kotak first"
        )

    runtime[
        "stock_scan_enabled"
    ] = True

    if (
        stock_feed_task
        and
        not stock_feed_task.done()
    ):
        return

    stock_feed_task = (
        asyncio.create_task(
            stock_feed_loop()
        )
    )


async def stop_stock_scan():
    global stock_feed_task

    runtime[
        "stock_scan_enabled"
    ] = False

    runtime[
        "stock_feed_connected"
    ] = False

    if (
        stock_feed_task
        and
        not stock_feed_task.done()
    ):
        stock_feed_task.cancel()

    stock_feed_task = None


# =========================================================
# TELEGRAM COMMANDS
# =========================================================
def index_readiness_text(symbol):
    return (
        f"{symbol}: "
        f"1M {len(candles_1m[symbol])}/{INDEX_NEED_1M}, "
        f"5M {len(candles_5m[symbol])}/{INDEX_NEED_5M}, "
        f"15M {len(candles_15m[symbol])}/{INDEX_NEED_15M}"
    )


def help_text():
    return (
        "👑 KING BRO — TELEGRAM ONLY\n\n"
        "/login 123456 — current Kotak TOTP login\n"
        "/status — feed + candle readiness\n"
        "/indexon — NIFTY/SENSEX signals ON\n"
        "/indexoff — NIFTY/SENSEX signals OFF\n"
        "/stockon — old 40-stock scanner ON\n"
        "/stockoff — stock scanner OFF\n"
        "/save — save candles to private Gist\n"
        "/test — Telegram test\n\n"
        "Index signal window: 09:30–15:30 IST.\n"
        "Original V7.3 index strategy is preserved."
    )


@app.post("/telegram/webhook")
async def telegram_webhook(
    request: Request
):
    if TELEGRAM_WEBHOOK_SECRET:
        received_secret = (
            request.headers.get(
                "X-Telegram-Bot-Api-Secret-Token",
                "",
            )
        )

        if (
            received_secret
            !=
            TELEGRAM_WEBHOOK_SECRET
        ):
            raise HTTPException(
                status_code=403,
                detail="Bad Telegram webhook secret",
            )

    update = await request.json()

    message = (
        update.get("message")
        or {}
    )

    chat = (
        message.get("chat")
        or {}
    )

    chat_id = str(
        chat.get("id")
        or ""
    )

    if (
        not TELEGRAM_CHAT_ID
        or
        chat_id
        !=
        str(TELEGRAM_CHAT_ID)
    ):
        return {"ok": True}

    text = str(
        message.get("text")
        or ""
    ).strip()

    message_id = (
        message.get(
            "message_id"
        )
    )

    if text.startswith("/login "):
        parts = text.split()

        if len(parts) != 2:
            await telegram_send(
                "Usage: /login 123456",
                with_keyboard=True,
            )
            return {"ok": True}

        totp = parts[1].strip()

        # Remove TOTP-bearing Telegram message quickly.
        if message_id:
            asyncio.create_task(
                delete_telegram_message(
                    chat_id,
                    message_id,
                )
            )

        try:
            await login_with_totp(
                totp
            )

            await telegram_send(
                "✅ KOTAK CONNECTED\n\n"
                + index_readiness_text(
                    "NIFTY 50"
                )
                + "\n"
                + index_readiness_text(
                    "SENSEX"
                )
                + "\n\n"
                + (
                    "✅ Previous candle history is ready."
                    if (
                        len(candles_1m["NIFTY 50"])
                        >= INDEX_NEED_1M
                        and
                        len(candles_5m["NIFTY 50"])
                        >= INDEX_NEED_5M
                        and
                        len(candles_15m["NIFTY 50"])
                        >= INDEX_NEED_15M
                        and
                        len(candles_1m["SENSEX"])
                        >= INDEX_NEED_1M
                        and
                        len(candles_5m["SENSEX"])
                        >= INDEX_NEED_5M
                        and
                        len(candles_15m["SENSEX"])
                        >= INDEX_NEED_15M
                    )
                    else
                    "⚠️ Candle history is not fully ready yet."
                )
                + "\nSignals are active only from 09:30 IST.",
                with_keyboard=True,
            )

        except Exception as exc:
            runtime[
                "last_error"
            ] = (
                f"{type(exc).__name__}: {exc}"
            )

            await telegram_send(
                f"❌ Kotak login failed: "
                f"{runtime['last_error']}",
                with_keyboard=True,
            )

        return {"ok": True}

    if text in {
        "/login",
        "/loginhelp",
    }:
        await telegram_send(
            "🔐 Login command:\n"
            "/login CURRENT_6_DIGIT_TOTP\n\n"
            "Example: /login 123456\n"
            "The bot tries to delete the TOTP message after reading it.",
            with_keyboard=True,
        )
        return {"ok": True}

    if text == "/status":
        await telegram_send(
            "👑 KING BRO STATUS\n\n"
            f"Broker: "
            f"{'CONNECTED' if runtime['broker_connected'] else 'OFFLINE'}\n"
            f"Index feed: "
            f"{'LIVE' if runtime['index_feed_connected'] else 'OFFLINE'}\n"
            f"Index signals: "
            f"{'ON' if runtime['index_scan_enabled'] else 'OFF'}\n"
            f"Signal window now: "
            f"{'ACTIVE' if signal_window_open() else 'INACTIVE'}\n\n"
            f"{index_readiness_text('NIFTY 50')}\n"
            f"{index_readiness_text('SENSEX')}\n\n"
            f"State source: "
            f"{runtime.get('state_source')}\n"
            f"Last state save: "
            f"{runtime.get('last_state_save_at')}\n"
            f"Last index signal: "
            f"{runtime.get('last_index_signal_at')}\n"
            f"Last index blocker: "
            f"{runtime.get('last_index_blocker')}\n\n"
            f"Stock scan: "
            f"{'ON' if runtime['stock_scan_enabled'] else 'OFF'}\n"
            f"Stock feed: "
            f"{'LIVE' if runtime['stock_feed_connected'] else 'OFFLINE'}\n"
            f"Stocks resolved: "
            f"{runtime['stock_resolved']}/{len(STOCK_UNIVERSE)}\n"
            f"Last stock signal: "
            f"{runtime.get('last_stock_signal_at')}\n"
            f"Last error: "
            f"{runtime.get('last_error')}",
            with_keyboard=True,
        )
        return {"ok": True}

    if text == "/indexon":
        runtime[
            "index_scan_enabled"
        ] = True

        await telegram_send(
            "✅ NIFTY + SENSEX SIGNALS ON",
            with_keyboard=True,
        )

        return {"ok": True}

    if text == "/indexoff":
        runtime[
            "index_scan_enabled"
        ] = False

        await telegram_send(
            "⏹ NIFTY + SENSEX SIGNALS OFF",
            with_keyboard=True,
        )

        return {"ok": True}

    if text == "/stockon":
        try:
            await start_stock_scan()

            await telegram_send(
                "✅ STOCK SCAN ON\n"
                f"Universe: {len(STOCK_UNIVERSE)} stocks\n"
                "Original old stock strategy active.",
                with_keyboard=True,
            )

        except Exception as exc:
            await telegram_send(
                f"❌ Stock scan could not start: "
                f"{type(exc).__name__}: {exc}",
                with_keyboard=True,
            )

        return {"ok": True}

    if text == "/stockoff":
        await stop_stock_scan()

        await telegram_send(
            "⏹ STOCK SCAN OFF",
            with_keyboard=True,
        )

        return {"ok": True}

    if text == "/save":
        ok = await save_state()

        await telegram_send(
            (
                "✅ Candle history saved to private Gist."
                if ok
                else
                "⚠️ State save failed / Gist not configured."
            ),
            with_keyboard=True,
        )

        return {"ok": True}

    if text == "/test":
        await telegram_send(
            "✅ KING BRO Telegram-only service is alive.",
            with_keyboard=True,
        )

        return {"ok": True}

    if text in {
        "/help",
        "/start",
        "/menu",
    }:
        await telegram_send(
            help_text(),
            with_keyboard=True,
        )
        return {"ok": True}

    return {"ok": True}


# =========================================================
# HTTP HEALTH / FALLBACK LOGIN
# =========================================================
@app.get("/")
async def root():
    return {
        "service":
            "KING BRO Telegram Original V6 Daily Auto",

        "version":
            APP_VERSION,

        "telegram_only":
            True,

        "strategy":
            "Original V7.3 multi-timeframe score engine",

        "index_warmup":
            {
                "1m": INDEX_NEED_1M,
                "5m": INDEX_NEED_5M,
                "15m": INDEX_NEED_15M,
                "method":
                    "restore previous candles from durable Gist; first empty-Gist session must warm up live",
            },

        "signal_window_ist":
            "09:30-15:30",
    }


@app.get("/health")
async def health():
    return {
        "ok": True,
        "version": APP_VERSION,

        "broker_connected":
            runtime[
                "broker_connected"
            ],

        "index_feed_connected":
            runtime[
                "index_feed_connected"
            ],

        "index_scan_enabled":
            runtime[
                "index_scan_enabled"
            ],

        "signal_window_active":
            signal_window_open(),

        "nifty": {
            "1m":
                len(
                    candles_1m[
                        "NIFTY 50"
                    ]
                ),

            "5m":
                len(
                    candles_5m[
                        "NIFTY 50"
                    ]
                ),

            "15m":
                len(
                    candles_15m[
                        "NIFTY 50"
                    ]
                ),
        },

        "sensex": {
            "1m":
                len(
                    candles_1m[
                        "SENSEX"
                    ]
                ),

            "5m":
                len(
                    candles_5m[
                        "SENSEX"
                    ]
                ),

            "15m":
                len(
                    candles_15m[
                        "SENSEX"
                    ]
                ),
        },

        "stock_scan_enabled":
            runtime[
                "stock_scan_enabled"
            ],

        "stock_feed_connected":
            runtime[
                "stock_feed_connected"
            ],

        "state_source":
            runtime.get(
                "state_source"
            ),

        "last_state_save_at":
            runtime.get(
                "last_state_save_at"
            ),

        "last_error":
            runtime.get(
                "last_error"
            ),
    }


@app.post("/login")
async def http_login(
    body: TotpRequest
):
    try:
        await login_with_totp(
            body.totp
        )

        return {
            "ok": True,
            "message":
                "Kotak connected",
        }

    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=
                f"{type(exc).__name__}: {exc}",
        )


# =========================================================
# STARTUP
# =========================================================
@app.on_event("startup")
async def startup():
    await restore_state()

    try:
        await setup_telegram()
    except Exception as exc:
        print(
            f"[TELEGRAM_SETUP_FAILED] "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )

    if (
        TELEGRAM_BOT_TOKEN
        and
        TELEGRAM_CHAT_ID
    ):
        await telegram_send(
            "👑 KING BRO Telegram-only backend ready.\n\n"
            f"{index_readiness_text('NIFTY 50')}\n"
            f"{index_readiness_text('SENSEX')}\n\n"
            "Send /login CURRENT_TOTP once in the morning.\n"
            "After login, live feed + automatic signal generation run for the session.\n"
            "Original V7.3 signal engine is locked.\n"
            "Signals: 09:30–15:30 IST.\n"
            "Previous candles restore from private Gist when available.",
            with_keyboard=True,
        )
