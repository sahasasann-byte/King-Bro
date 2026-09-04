import asyncio
import re
import json
import os
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from datetime import datetime, timezone, date, time as dt_time
from zoneinfo import ZoneInfo
from typing import Optional, Any
from collections import deque

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from neo_api_client import NeoAPI
from neo_api_client.websocket.feed import (
    WsToken,
    SFeedIndex,
    SFeedScrip,
)

from config import settings


# =========================================================
# APP
# =========================================================

app = FastAPI(
    title="King Bro Terminal API",
    version="7.7.0",
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


class OptionSearchRequest(BaseModel):
    exchange_segment: str = "nse_fo"
    symbol: str
    expiry: str = ""
    option_type: str = ""
    strike_price: str = ""


class OptionInspectRequest(BaseModel):
    instrument_token: str
    exchange_segment: str = "nse_fo"


class AttachOptionRequest(BaseModel):
    instrument_token: str
    exchange_segment: str = "nse_fo"


class ManualOrderRequest(BaseModel):
    exchange_segment: str
    trading_symbol: str
    transaction_type: str
    quantity: int = Field(gt=0)
    product: str = "MIS"
    order_type: str = "MKT"
    price: float = 0
    validity: str = "DAY"
    confirm: bool = False
    client_request_id: str = ""


class SquareOffRequest(BaseModel):
    exchange_segment: str
    trading_symbol: str
    quantity: int = Field(gt=0)
    current_net_quantity: int
    product: str = "MIS"
    confirm: bool = False


class SquareOffAllRequest(BaseModel):
    confirm_text: str


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
stock_feed_task: Optional[asyncio.Task] = None
runtime_supervisor_task: Optional[asyncio.Task] = None
render_keepalive_task: Optional[asyncio.Task] = None
background_tasks: set[asyncio.Task] = set()

IST = ZoneInfo("Asia/Kolkata")

runtime_state = {
    "process_started_at": datetime.now(timezone.utc).isoformat(),
    "process_boot_id": uuid.uuid4().hex[:12],
    "supervisor_running": False,
    "keepalive_running": False,
    "keepalive_enabled": bool(settings.KINGBRO_KEEPALIVE_ENABLED),
    "keepalive_url_detected": False,
    "last_keepalive_at": None,
    "last_keepalive_error": None,
    "feed_restart_count": 0,
    "last_feed_restart_at": None,
    "last_feed_restart_reason": None,
    "last_feed_stale_age_seconds": None,
    "auth_relogin_required": False,
    "restart_login_required": False,
    "broker_authenticated_at": None,
    "feed_connected_at": None,
    "last_runtime_notice_at": None,
    "last_runtime_notice_error": None,
}

signal_diagnostics = {
    "evaluations": 0,
    "technical_actionable": 0,
    "final_actionable": 0,
    "option_filtered": 0,
    "option_errors": 0,
    "warming_up": 0,
    "watch": 0,
    "no_trade": 0,
    "last_evaluation_at": None,
    "last_by_symbol": {},
}


def _spawn_background(coro):
    """Run non-critical I/O without blocking Kotak market tick consumption."""
    task = asyncio.create_task(coro)
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)
    return task


# =========================================================
# SIGNAL ENGINE V1 — NIFTY 50 + SENSEX ONLY
# =========================================================

SIGNAL_SYMBOLS = ("NIFTY 50", "SENSEX")
MAX_1M_CANDLES = 300
MAX_5M_CANDLES = 200
MAX_15M_CANDLES = 200

candles_1m = {
    s: deque(maxlen=MAX_1M_CANDLES)
    for s in SIGNAL_SYMBOLS
}
candles_5m = {
    s: deque(maxlen=MAX_5M_CANDLES)
    for s in SIGNAL_SYMBOLS
}
candles_15m = {
    s: deque(maxlen=MAX_15M_CANDLES)
    for s in SIGNAL_SYMBOLS
}
active_1m = {s: None for s in SIGNAL_SYMBOLS}
active_5m = {s: None for s in SIGNAL_SYMBOLS}
active_15m = {s: None for s in SIGNAL_SYMBOLS}

signals: dict[str, dict[str, Any]] = {}

signal_history = deque(maxlen=100)

scanner_state = {
    "index_scan_enabled": True,
    "stock_scan_enabled": False,
    "stock_scan_running": False,
    "stock_resolved": 0,
    "stock_unresolved": 0,
    "stock_last_error": None,
}

# Fixed V7.1 intraday universe.
# No price filter. Symbols are resolved to current Kotak NSE cash tokens at runtime.
STOCK_UNIVERSE = (
    "RELIANCE",
    "HDFCBANK",
    "ICICIBANK",
    "SBIN",
    "AXISBANK",
    "KOTAKBANK",
    "INDUSINDBK",
    "BAJFINANCE",
    "TATAMOTORS",
    "M&M",
    "MARUTI",
    "EICHERMOT",
    "TVSMOTOR",
    "TATASTEEL",
    "HINDALCO",
    "JSWSTEEL",
    "ADANIENT",
    "ADANIPORTS",
    "LT",
    "BEL",
    "HAL",
    "BHEL",
    "RVNL",
    "IRFC",
    "PFC",
    "RECLTD",
    "POWERGRID",
    "NTPC",
    "TATAPOWER",
    "COALINDIA",
    "ONGC",
    "BPCL",
    "IOC",
    "ITC",
    "TRENT",
    "DLF",
    "INFY",
    "TCS",
    "BHARTIARTL",
    "SUNPHARMA",
)

stock_token_map: dict[str, dict[str, str]] = {}
stock_latest: dict[str, dict[str, Any]] = {}

STOCK_MAX_1M = 180
STOCK_MAX_5M = 120
STOCK_MAX_15M = 80

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

stock_signals: dict[str, dict[str, Any]] = {}

# Manual execution only. Signals never call place_order().
execution_state = {
    "mode": "MANUAL_ONLY",
    "auto_order_enabled": False,
    "last_order_at": None,
    "last_order_error": None,
}

recent_manual_requests: dict[str, dict[str, Any]] = {}

option_oi_history: dict[str, dict[str, Any]] = {}


# =========================================================
# TELEGRAM SIGNAL ALERTS — V7.3
# =========================================================
# Environment variables:
# TELEGRAM_BOT_TOKEN = token from BotFather
# TELEGRAM_CHAT_ID   = numeric chat/group/channel id
#
# Signal alerts only. This module NEVER places an order.

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

telegram_state = {
    "configured": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
    "enabled": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
    "last_sent_at": None,
    "last_error": None,
    "sent_count": 0,
    "failed_count": 0,
    "last_attempt_at": None,
    "last_skip_reason": None,
}

# key -> last alert timestamp. Prevents repeated alerts for the same setup.
telegram_alert_cache: dict[str, float] = {}
TELEGRAM_ALERT_COOLDOWN_SECONDS = 15 * 60
TELEGRAM_SEND_RETRIES = 3
TELEGRAM_RETRY_DELAY_SECONDS = 1.25


def _telegram_signal_key(signal: dict[str, Any]) -> str:
    """Return the cooldown identity for an alert setup.

    Grade is intentionally NOT part of the key. A STRONG -> A+ (or A+ ->
    STRONG) reclassification for the same symbol/direction/contract is still
    the same trade idea during the cooldown window and must not generate a
    duplicate Telegram alert.
    """
    contract = signal.get("option_contract") or {}
    return "|".join([
        str(signal.get("symbol") or ""),
        str(signal.get("direction") or ""),
        str(contract.get("instrument_token") or contract.get("display_symbol") or ""),
    ])


def _telegram_message(signal: dict[str, Any]) -> str:
    contract = signal.get("option_contract") or {}
    reasons = signal.get("reasons") or []

    lines = [
        "👑 THE RAAJA BRO — SIGNAL",
        "",
        f"📊 {signal.get('symbol', '-')}",
        f"🎯 {signal.get('direction', '-')}  |  {signal.get('grade', '-')}",
        f"⭐ Score: {signal.get('score', '-')}/100",
    ]

    option_name = contract.get("display_symbol")
    if option_name:
        lines.append(f"🧾 Option: {option_name}")

    if signal.get("underlying_ltp") is not None:
        lines.append(f"Index/Spot: {signal.get('underlying_ltp')}")
    if signal.get("option_ltp") is not None:
        lines.append(f"Option LTP: ₹{signal.get('option_ltp')}")
    if signal.get("entry") is not None:
        lines.append(f"Entry: ₹{signal.get('entry')}")
    if signal.get("stop_loss") is not None:
        lines.append(f"SL: ₹{signal.get('stop_loss')}")
    if signal.get("target_1") is not None:
        lines.append(f"T1: ₹{signal.get('target_1')}")
    if signal.get("target_2") is not None:
        lines.append(f"T2: ₹{signal.get('target_2')}")

    if reasons:
        lines.extend(["", "Confirmations:"])
        lines.extend(f"• {x}" for x in reasons[:8])

    warnings = signal.get("warnings") or []
    if warnings:
        lines.extend(["", "Option data note:"])
        lines.extend(f"• {x}" for x in warnings[:2])

    if signal.get("engine") == "classic_breakout_v1":
        lines.append("Engine: Classic 5M Trend-Aligned Breakout • Min R:R 1:1.85")

    lines.extend([
        "",
        "⚠️ MANUAL ORDER ONLY — no automatic execution.",
        f"Time: {signal.get('generated_at') or datetime.now(timezone.utc).isoformat()}",
    ])
    return "\n".join(lines)


def _telegram_send_sync(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError(
            "Telegram is not configured. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID."
        )

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = urllib.parse.urlencode({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": "true",
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    with urllib.request.urlopen(req, timeout=12) as response:
        raw = response.read().decode("utf-8")
        data = json.loads(raw)

    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")

    return data


async def _send_runtime_telegram_notice(message: str):
    """Operational notice only; never treated as a trading signal."""
    if not telegram_state.get("configured"):
        return False
    try:
        await asyncio.to_thread(_telegram_send_sync, message)
        runtime_state["last_runtime_notice_at"] = datetime.now(timezone.utc).isoformat()
        runtime_state["last_runtime_notice_error"] = None
        print("[RUNTIME_TELEGRAM_NOTICE_SENT]", flush=True)
        return True
    except Exception as exc:
        runtime_state["last_runtime_notice_error"] = f"{type(exc).__name__}: {exc}"
        print(f"[RUNTIME_TELEGRAM_NOTICE_FAILED] {type(exc).__name__}: {exc}", flush=True)
        return False


async def _maybe_send_telegram_signal(signal: dict[str, Any]):
    """Deliver an already-qualified signal to Telegram.

    V7.7 alerts confirmed classic breakouts. Grades A+/STRONG/BREAKOUT are eligible;
    option enrichment is informative and is not a hard veto.
    """
    symbol = str(signal.get("symbol") or "-")
    grade = signal.get("grade")

    if not telegram_state["enabled"]:
        telegram_state["last_skip_reason"] = "telegram_disabled"
        return False
    if not signal.get("actionable"):
        telegram_state["last_skip_reason"] = f"{symbol}:not_actionable:{grade}"
        return False
    if grade not in {"A+", "STRONG", "BREAKOUT"}:
        telegram_state["last_skip_reason"] = f"{symbol}:grade_not_alertable:{grade}"
        return False

    key = _telegram_signal_key(signal)
    now_ts = datetime.now(timezone.utc).timestamp()
    previous_ts = telegram_alert_cache.get(key)

    if previous_ts and (now_ts - previous_ts) < TELEGRAM_ALERT_COOLDOWN_SECONDS:
        telegram_state["last_skip_reason"] = f"{symbol}:cooldown"
        return False

    message = _telegram_message(signal)
    last_exc = None
    for attempt in range(1, TELEGRAM_SEND_RETRIES + 1):
        telegram_state["last_attempt_at"] = datetime.now(timezone.utc).isoformat()
        try:
            await asyncio.to_thread(_telegram_send_sync, message)
            telegram_alert_cache[key] = datetime.now(timezone.utc).timestamp()
            telegram_state["last_sent_at"] = datetime.now(timezone.utc).isoformat()
            telegram_state["last_error"] = None
            telegram_state["last_skip_reason"] = None
            telegram_state["sent_count"] += 1
            print(f"[TELEGRAM_SENT] {symbol} {signal.get('direction')} {grade} score={signal.get('score')} attempt={attempt}", flush=True)
            return True
        except Exception as exc:
            last_exc = exc
            telegram_state["last_error"] = f"{type(exc).__name__}: {exc}"
            print(f"[TELEGRAM_SEND_FAILED] {symbol} attempt={attempt}/{TELEGRAM_SEND_RETRIES}: {type(exc).__name__}: {exc}", flush=True)
            if attempt < TELEGRAM_SEND_RETRIES:
                await asyncio.sleep(TELEGRAM_RETRY_DELAY_SECONDS * attempt)

    telegram_state["failed_count"] += 1
    telegram_state["last_skip_reason"] = f"{symbol}:delivery_failed_after_retries"
    if last_exc is not None:
        telegram_state["last_error"] = f"{type(last_exc).__name__}: {last_exc}"
    return False


@app.get("/api/telegram/status")
async def telegram_status():
    return {
        **telegram_state,
        "bot_token_configured": bool(TELEGRAM_BOT_TOKEN),
        "chat_id_configured": bool(TELEGRAM_CHAT_ID),
        "cooldown_seconds": TELEGRAM_ALERT_COOLDOWN_SECONDS,
    }


@app.post("/api/telegram/test")
async def telegram_test():
    if not telegram_state["configured"]:
        raise HTTPException(
            status_code=400,
            detail="Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID first.",
        )
    try:
        message = (
            "👑 THE RAAJA BRO — Telegram connected.\n"
            "Signal alerts are ON.\n"
            "MANUAL ORDER ONLY — auto execution remains OFF."
        )
        result = await asyncio.to_thread(_telegram_send_sync, message)
        telegram_state["last_sent_at"] = datetime.now(timezone.utc).isoformat()
        telegram_state["last_error"] = None
        telegram_state["sent_count"] += 1
        return {"status": "ok", "telegram": telegram_state, "message_id": (result.get("result") or {}).get("message_id")}
    except Exception as exc:
        telegram_state["last_error"] = f"{type(exc).__name__}: {exc}"
        raise HTTPException(
            status_code=400,
            detail={"error_type": type(exc).__name__, "message": str(exc)},
        )


# =========================================================
# CANDLE PERSISTENCE / RESTORE — V6.1
# =========================================================

# Auto-use a Render persistent disk if mounted at /var/data.
# Otherwise fall back to /tmp (not durable across a new Render instance).
_PERSIST_ROOT = (
    Path("/var/data/kingbro")
    if Path("/var/data").exists()
    else Path("/tmp/kingbro")
)
_PERSIST_ROOT.mkdir(parents=True, exist_ok=True)

CANDLE_STATE_FILE = _PERSIST_ROOT / "candle_state.json"

persistence_status = {
    "path": str(CANDLE_STATE_FILE),
    "persistent_disk_detected": str(CANDLE_STATE_FILE).startswith("/var/data/"),
    "restored": False,
    "last_saved_at": None,
    "last_restore_at": None,
    "last_error": None,
}


# Latest inspected option intelligence, keyed by "exchange|token".
option_intelligence: dict[str, dict[str, Any]] = {}

# Daily CPR/Fib Pivot cache — V6.2.
# Values are calculated only from completed real candles already collected.
daily_levels = {
    s: {
        "ready": False,
        "pivot": None,
        "cpr": None,
        "classic": None,
        "fib": None,
        "reference_session": None,
        "source": None,
        "reason": "Need completed candles spanning at least two UTC sessions.",
    }
    for s in SIGNAL_SYMBOLS
}



def _serialisable_candle_state():
    return {
        "version": 1,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "candles_1m": {
            symbol: list(candles_1m[symbol])
            for symbol in SIGNAL_SYMBOLS
        },
        "candles_5m": {
            symbol: list(candles_5m[symbol])
            for symbol in SIGNAL_SYMBOLS
        },
        "candles_15m": {
            symbol: list(candles_15m[symbol])
            for symbol in SIGNAL_SYMBOLS
        },
        "active_1m": {
            symbol: active_1m[symbol]
            for symbol in SIGNAL_SYMBOLS
        },
        "active_5m": {
            symbol: active_5m[symbol]
            for symbol in SIGNAL_SYMBOLS
        },
        "active_15m": {
            symbol: active_15m[symbol]
            for symbol in SIGNAL_SYMBOLS
        },
    }


def _save_candle_state_sync():
    try:
        payload = _serialisable_candle_state()
        tmp_file = CANDLE_STATE_FILE.with_suffix(".tmp")

        tmp_file.write_text(
            json.dumps(payload, separators=(",", ":")),
            encoding="utf-8",
        )

        os.replace(tmp_file, CANDLE_STATE_FILE)

        persistence_status["last_saved_at"] = payload["saved_at"]
        persistence_status["last_error"] = None

    except Exception as exc:
        persistence_status["last_error"] = (
            f"{type(exc).__name__}: {exc}"
        )


async def save_candle_state():
    await asyncio.to_thread(_save_candle_state_sync)


def _restore_candle_state_sync():
    persistence_status["last_restore_at"] = (
        datetime.now(timezone.utc).isoformat()
    )

    if not CANDLE_STATE_FILE.exists():
        persistence_status["restored"] = False
        return

    try:
        payload = json.loads(
            CANDLE_STATE_FILE.read_text(encoding="utf-8")
        )

        for symbol in SIGNAL_SYMBOLS:
            one = payload.get("candles_1m", {}).get(symbol, [])
            five = payload.get("candles_5m", {}).get(symbol, [])
            fifteen = payload.get("candles_15m", {}).get(symbol, [])

            candles_1m[symbol].clear()
            candles_5m[symbol].clear()
            candles_15m[symbol].clear()

            for candle in one[-MAX_1M_CANDLES:]:
                if isinstance(candle, dict):
                    candles_1m[symbol].append(candle)

            for candle in five[-MAX_5M_CANDLES:]:
                if isinstance(candle, dict):
                    candles_5m[symbol].append(candle)

            for candle in fifteen[-MAX_15M_CANDLES:]:
                if isinstance(candle, dict):
                    candles_15m[symbol].append(candle)

            a1 = payload.get("active_1m", {}).get(symbol)
            a5 = payload.get("active_5m", {}).get(symbol)
            a15 = payload.get("active_15m", {}).get(symbol)

            active_1m[symbol] = a1 if isinstance(a1, dict) else None
            active_5m[symbol] = a5 if isinstance(a5, dict) else None
            active_15m[symbol] = a15 if isinstance(a15, dict) else None

        persistence_status["restored"] = True
        persistence_status["last_error"] = None

    except Exception as exc:
        persistence_status["restored"] = False
        persistence_status["last_error"] = (
            f"{type(exc).__name__}: {exc}"
        )


@app.on_event("startup")
async def restore_candle_state_on_startup():
    await asyncio.to_thread(_restore_candle_state_sync)
    _start_runtime_supervisors()

    # A new Python process cannot retain the authenticated NeoAPI object.
    # Make this explicit instead of silently looking like a random logout.
    if _keepalive_window_active():
        runtime_state["restart_login_required"] = True
        await set_status(
            broker_connected=False,
            feed_connected=False,
            last_error="SERVER RESTARTED / TOTP LOGIN REQUIRED",
        )
        if telegram_state.get("configured"):
            _spawn_background(_send_runtime_telegram_notice(
                "⚠️ KING BRO backend restarted during market hours.\n"
                "Kotak live session is not restored automatically.\n"
                "Open KING BRO and enter the current TOTP once to resume NIFTY/SENSEX signals."
            ))


@app.on_event("shutdown")
async def stop_runtime_supervisors_on_shutdown():
    global runtime_supervisor_task, render_keepalive_task
    for task in (runtime_supervisor_task, render_keepalive_task):
        if task and not task.done():
            task.cancel()
    for task in list(background_tasks):
        if not task.done():
            task.cancel()

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



def _session_key_from_candle(candle):
    ts = candle.get("ts")
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(
            float(ts), tz=timezone.utc
        ).date().isoformat()
    except Exception:
        return None


def _previous_session_hlc(candles):
    """
    Derive previous completed session H/L/C from real stored candles.
    Current session is excluded. No synthetic previous-day values.
    """
    sessions = {}

    for candle in candles:
        day = _session_key_from_candle(candle)
        if day is None:
            continue
        sessions.setdefault(day, []).append(candle)

    if len(sessions) < 2:
        return None

    days = sorted(sessions)
    previous_day = days[-2]
    rows = sessions[previous_day]

    highs = [number(c.get("high")) for c in rows]
    lows = [number(c.get("low")) for c in rows]
    closes = [number(c.get("close")) for c in rows]

    highs = [x for x in highs if x is not None]
    lows = [x for x in lows if x is not None]
    closes = [x for x in closes if x is not None]

    if not highs or not lows or not closes:
        return None

    return {
        "session": previous_day,
        "high": max(highs),
        "low": min(lows),
        "close": closes[-1],
        "candle_count": len(rows),
    }


def _pivot_levels(high, low, close):
    h = number(high)
    l = number(low)
    c = number(close)

    if h is None or l is None or c is None or h < l:
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
            "width": round(top - bottom, 2),
        },
        "classic": {
            "r1": round((2.0 * pivot) - l, 2),
            "s1": round((2.0 * pivot) - h, 2),
        },
        "fib": {
            "r1": round(pivot + 0.382 * rng, 2),
            "r2": round(pivot + 0.618 * rng, 2),
            "r3": round(pivot + rng, 2),
            "s1": round(pivot - 0.382 * rng, 2),
            "s2": round(pivot - 0.618 * rng, 2),
            "s3": round(pivot - rng, 2),
        },
    }


def _refresh_daily_levels(symbol):
    reference = _previous_session_hlc(
        list(candles_1m[symbol])
    )

    if reference is None:
        daily_levels[symbol] = {
            "ready": False,
            "pivot": None,
            "cpr": None,
            "classic": None,
            "fib": None,
            "reference_session": None,
            "source": None,
            "reason": (
                "Need completed real 1m candles spanning at least "
                "two UTC sessions."
            ),
        }
        return daily_levels[symbol]

    levels = _pivot_levels(
        reference["high"],
        reference["low"],
        reference["close"],
    )

    if levels is None:
        return daily_levels[symbol]

    daily_levels[symbol] = {
        "ready": True,
        **levels,
        "reference_session": reference["session"],
        "source": {
            "high": round(reference["high"], 2),
            "low": round(reference["low"], 2),
            "close": round(reference["close"], 2),
            "candle_count": reference["candle_count"],
        },
        "reason": None,
    }

    return daily_levels[symbol]


def _five_minute_pivot(five):
    """
    Intraday context from the last completed 5m candle.
    This is not a replacement for previous-day CPR.
    """
    if not five:
        return {
            "ready": False,
            "reason": "No completed 5m candle yet.",
        }

    c = five[-1]
    levels = _pivot_levels(
        c.get("high"),
        c.get("low"),
        c.get("close"),
    )

    if levels is None:
        return {
            "ready": False,
            "reason": "Last completed 5m candle is invalid.",
        }

    return {
        "ready": True,
        **levels,
        "source_candle_ts": c.get("ts"),
    }


def _price_vs_levels(price, levels):
    p = number(price)

    if p is None or not levels or not levels.get("ready"):
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

def _indicator_snapshot(symbol):
    one = list(candles_1m[symbol])
    five = list(candles_5m[symbol])
    fifteen = list(candles_15m[symbol])

    c1 = [c["close"] for c in one]
    c5 = [c["close"] for c in five]
    c15 = [c["close"] for c in fifteen]

    daily = _refresh_daily_levels(symbol)
    five_pivot = _five_minute_pivot(five)
    current_price = latest.get(symbol, {}).get("ltp")

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
        "fifteen_minute": {
            "count": len(fifteen),
            "ema9": _ema(c15, 9),
            "ema21": _ema(c15, 21),
            "ma20": _sma(c15, 20),
            "rsi14": _rsi(c15, 14),
            "price_action": _price_action(fifteen),
            "breakout": _breakout(fifteen),
        },
        "daily_levels": daily,
        "daily_level_context": _price_vs_levels(
            current_price, daily
        ),
        "five_minute_levels": five_pivot,
        "five_minute_level_context": _price_vs_levels(
            current_price, five_pivot
        ),
        "vwap": {
            "ready": False,
            "reason": "True index VWAP needs usable traded volume; not fabricated.",
        },
        "order_flow": {
            "ready": False,
            "reason": "Depth/order-flow module not enabled yet.",
        },
        "options": {
            "ready": neo_client is not None,
            "mode": "AUTO_ATM_NEAREST_EXPIRY",
            "inputs": [
                "Kotak option LTP",
                "open interest",
                "liquidity score",
            ],
            "note": (
                "Greeks and India VIX are not used until dedicated "
                "validated inputs are wired."
            ),
        },
    }


def _direction_score(snapshot, direction):
    one = snapshot["one_minute"]
    five = snapshot["five_minute"]
    fifteen = snapshot["fifteen_minute"]

    score = 0
    reasons = []
    blockers = []

    # FAST-SAFE SCALPING WARM-UP
    #
    # Do not block the whole signal engine for 21 x 5m bars (105 min)
    # and 9 x 15m bars (135 min). For a scalping terminal that made
    # every fresh Render restart look "dead" for far too long.
    #
    # Minimum context:
    #   - 22 completed 1m bars => EMA21 + RSI/Williams context available
    #   - 3 completed 5m bars  => real 5m price-action context available
    #
    # 5m EMA21 and 15m confirmations remain OPTIONAL score boosters.
    # They start contributing automatically as those bars accumulate.
    # No synthetic/history values are invented.
    if one["count"] < 22:
        blockers.append(f"1m warm-up {one['count']}/22")
    if five["count"] < 3:
        blockers.append(f"5m warm-up {five['count']}/3")

    if five["price_action"] == direction:
        score += 20
        reasons.append("5M price action")

    if fifteen["price_action"] == direction:
        score += 10
        reasons.append("15M higher-timeframe price action")

    if fifteen["ema9"] is not None and fifteen["ema21"] is not None:
        ok15 = (
            fifteen["ema9"] > fifteen["ema21"]
            if direction == "BULLISH"
            else fifteen["ema9"] < fifteen["ema21"]
        )
        if ok15:
            score += 10
            reasons.append("15M EMA 9/21 higher-timeframe confirmation")

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

    daily_ctx = snapshot.get("daily_level_context") or {}
    if daily_ctx.get("ready"):
        cpr_pos = daily_ctx.get("cpr_position")
        fib_pos = daily_ctx.get("fib_position")

        if direction == "BULLISH":
            if cpr_pos == "ABOVE_CPR":
                score += 5
                reasons.append("Above daily CPR")
            if fib_pos == "ABOVE_R1":
                score += 5
                reasons.append("Above daily Fib R1")
        else:
            if cpr_pos == "BELOW_CPR":
                score += 5
                reasons.append("Below daily CPR")
            if fib_pos == "BELOW_S1":
                score += 5
                reasons.append("Below daily Fib S1")

    five_ctx = snapshot.get("five_minute_level_context") or {}
    if five_ctx.get("ready"):
        cpr_pos = five_ctx.get("cpr_position")

        if direction == "BULLISH" and cpr_pos == "ABOVE_CPR":
            score += 5
            reasons.append("Above 5M pivot CPR")
        elif direction == "BEARISH" and cpr_pos == "BELOW_CPR":
            score += 5
            reasons.append("Below 5M pivot CPR")

    # Preserve the original 0-100 public score scale.
    score = min(score, 100)

    return score, reasons, blockers




def _classic_breakout_signal(symbol: str, rows: list[dict[str, Any]], lookback: int = 20, min_rr: float = 1.85):
    """KING BRO classic trend-aligned breakout engine.

    This restores the earlier BaselineSignalEngine behavior for NIFTY/SENSEX:
    - completed 5-minute candles only
    - 20-candle resistance/support lookback
    - fast 5-close mean versus slow 20-close mean
    - close must break the prior 20-bar extreme in the trend direction
    - ATR-like risk from recent candle ranges
    - minimum target R:R 1:1.85 and T2 at 2.30R

    There is deliberately no separate min-score veto. A confirmed breakout is
    the signal, exactly as in the classic engine; the 60-95 score ranks its
    strength for display/Telegram.
    """
    if len(rows) < lookback + 2:
        return {
            "status": "REJECTED",
            "reason": "INSUFFICIENT_HISTORY",
            "need": lookback + 2,
            "have": len(rows),
        }

    recent = rows[-(lookback + 1):]
    prev = recent[:-1]
    last = recent[-1]

    closes = [float(x["close"]) for x in prev]
    highs = [float(x["high"]) for x in prev]
    lows = [float(x["low"]) for x in prev]
    entry = float(last["close"])

    if entry <= 0:
        return {"status": "REJECTED", "reason": "INVALID_PRICE"}

    fast = sum(closes[-5:]) / 5
    slow = sum(closes) / len(closes)
    resistance = max(highs)
    support = min(lows)
    ranges = [max(0.0, h - l) for h, l in zip(highs, lows)]
    atr_like = (sum(ranges[-14:]) / len(ranges[-14:])) if ranges else 0.0

    if atr_like <= 0:
        return {"status": "REJECTED", "reason": "NO_VOLATILITY"}

    if entry > resistance and fast > slow:
        side = "BUY"
        direction = "CALL"
        stop = max(float(last["low"]), entry - 1.25 * atr_like)
        risk = entry - stop
        target1 = entry + min_rr * risk
        target2 = entry + 2.30 * risk
        broken_level = resistance
    elif entry < support and fast < slow:
        side = "SELL"
        direction = "PUT"
        stop = min(float(last["high"]), entry + 1.25 * atr_like)
        risk = stop - entry
        target1 = entry - min_rr * risk
        target2 = entry - 2.30 * risk
        broken_level = support
    else:
        return {
            "status": "REJECTED",
            "reason": "NO_CONFIRMED_BREAKOUT",
            "fast": round(fast, 4),
            "slow": round(slow, 4),
            "resistance": round(resistance, 4),
            "support": round(support, 4),
            "entry": round(entry, 4),
        }

    if risk <= 0 or risk / entry > 0.08:
        return {"status": "REJECTED", "reason": "RISK_OUT_OF_RANGE"}

    trend_strength = min(20, int(abs(fast - slow) / entry * 10000))
    breakout_strength = min(20, int(abs(entry - broken_level) / atr_like * 10))
    score = min(95, 60 + trend_strength + breakout_strength)

    return {
        "status": "SIGNAL",
        "signal": {
            "symbol": symbol,
            "side": side,
            "direction": direction,
            "entry": round(entry, 4),
            "stop_loss": round(stop, 4),
            "target_1": round(target1, 4),
            "target_2": round(target2, 4),
            "rr": min_rr,
            "score": score,
            "reason": "trend-aligned breakout",
            "trend_strength": trend_strength,
            "breakout_strength": breakout_strength,
            "fast_mean_5": round(fast, 4),
            "slow_mean_20": round(slow, 4),
            "resistance": round(resistance, 4),
            "support": round(support, 4),
            "atr_like": round(atr_like, 4),
            "timeframe_sec": 300,
            "source": "classic_breakout_v1",
        },
    }

def _classify_option_oi(display_symbol, ltp, oi):
    if not display_symbol or ltp is None or oi is None:
        return {
            "ready": False,
            "classification": "UNKNOWN",
            "oi_change": None,
            "price_change": None,
        }

    key = str(display_symbol)
    prev = option_oi_history.get(key)

    current = {
        "ltp": float(ltp),
        "oi": float(oi),
        "at": datetime.now(timezone.utc).isoformat(),
    }
    option_oi_history[key] = current

    if not prev:
        return {
            "ready": False,
            "classification": "BASELINE_CAPTURED",
            "oi_change": None,
            "price_change": None,
        }

    oi_change = current["oi"] - float(prev["oi"])
    price_change = current["ltp"] - float(prev["ltp"])

    if oi_change > 0 and price_change > 0:
        cls = "LONG_BUILDUP"
    elif oi_change > 0 and price_change < 0:
        cls = "SHORT_BUILDUP"
    elif oi_change < 0 and price_change > 0:
        cls = "SHORT_COVERING"
    elif oi_change < 0 and price_change < 0:
        cls = "LONG_UNWINDING"
    else:
        cls = "FLAT"

    return {
        "ready": True,
        "classification": cls,
        "oi_change": round(oi_change, 2),
        "price_change": round(price_change, 2),
        "previous": prev,
        "current": current,
    }

def _option_trade_plan(option_ltp, option_data):
    """
    Conservative signal-only trade plan derived from the selected option LTP.
    No order is placed. Levels are rounded to 0.05.
    """
    if option_ltp is None or option_ltp <= 0:
        return None

    def tick(x):
        return round(round(float(x) / 0.05) * 0.05, 2)

    # ATR for the option is not available yet, so use percentage risk levels.
    # These are presentation/risk-plan levels, not guaranteed outcomes.
    entry = tick(option_ltp)
    stop = tick(option_ltp * 0.85)
    risk = max(entry - stop, 0.05)
    t1 = tick(entry + (risk * 1.85))
    t2 = tick(entry + (risk * 2.30))

    return {
        "entry": entry,
        "stop_loss": stop,
        "target_1": t1,
        "target_2": t2,
        "risk_per_unit": tick(risk),
        "rr_target_1": "1:1.85",
        "rr_target_2": "1:2.30",
        "basis": "selected option LTP; 15% signal-only risk model; classic breakout R:R",
    }


async def _attach_auto_option_to_signal(signal):
    """Attach the nearest-expiry ATM option as enrichment, never as a veto.

    V7.7 restores the classic breakout engine as the primary decision maker.
    Kotak option LTP/OI/liquidity remains valuable for contract selection and
    display, but an incomplete option quote must not kill a confirmed index
    breakout. This directly avoids the earlier score-85 -> OPTION_NOT_READY
    missed-alert path.
    """
    if not signal.get("actionable"):
        return signal

    signal.setdefault("warnings", [])
    signal["option_status"] = "PENDING"

    try:
        discovery = await asyncio.wait_for(
            auto_discover_option(
                signal["symbol"],
                signal["direction"],
            ),
            timeout=max(5, int(settings.KINGBRO_OPTION_CONFIRM_TIMEOUT_SECONDS)),
        )

        signal["option_discovery"] = discovery

        if not discovery.get("ready"):
            # Keep the classic technical signal alive. When Kotak search found
            # a nearest-expiry candidate but quotes lacked LTP, still expose
            # the contract identity so the alert/UI is useful.
            checked = discovery.get("candidates_checked") or []
            if checked:
                candidate = checked[0]
                q = candidate.get("quote") or {}
                signal["option_contract"] = {
                    "instrument_token": candidate.get("instrument_token"),
                    "exchange_segment": discovery.get("exchange_segment"),
                    "display_symbol": candidate.get("trading_symbol"),
                    "expiry": candidate.get("expiry"),
                    "strike_price": candidate.get("strike_price"),
                    "option_type": candidate.get("option_type"),
                }
                if q:
                    signal["option_intelligence"] = q
                    signal["option_ltp"] = q.get("ltp")
                    signal["option_quality_score"] = q.get("liquidity_score") or 0

            reason = discovery.get("reason", "Option discovery not ready.")
            signal["warnings"].append(reason)
            signal["option_status"] = "QUOTE_NOT_READY"
            signal["option_quality_pass"] = False
            return signal

        selected = discovery["selected"]
        quote = selected.get("quote") or {}
        option_ltp = quote.get("ltp")
        liquidity_score = quote.get("liquidity_score") or 0
        oi = quote.get("open_interest")

        signal["option_contract"] = {
            "instrument_token": selected.get("instrument_token"),
            "exchange_segment": discovery.get("exchange_segment"),
            "display_symbol": selected.get("trading_symbol"),
            "expiry": selected.get("expiry"),
            "strike_price": selected.get("strike_price"),
            "option_type": selected.get("option_type"),
        }
        signal["option_ltp"] = option_ltp
        signal["option_intelligence"] = quote
        signal["option_quality_score"] = liquidity_score
        signal["oi_intelligence"] = _classify_option_oi(
            selected.get("trading_symbol"),
            option_ltp,
            oi,
        )
        signal["option_quality_pass"] = (
            option_ltp is not None
            and option_ltp > 0
            and oi is not None
            and oi > 0
            and liquidity_score >= 60
        )

        if not signal["option_quality_pass"]:
            signal["warnings"].append(
                "Option quote is usable, but OI/liquidity confirmation is below the old V7 filter. "
                "Classic breakout signal remains active."
            )
            signal["option_status"] = "QUALITY_WARNING"
        else:
            signal["option_status"] = "READY"

        plan = _option_trade_plan(option_ltp, quote)
        if plan:
            signal.update(plan)

        return signal

    except Exception as exc:
        signal["warnings"].append(
            f"Option enrichment failed: {type(exc).__name__}: {exc}"
        )
        signal["option_status"] = "ERROR"
        signal["option_quality_pass"] = False
        return signal


async def _evaluate_signal(symbol):
    snap = _indicator_snapshot(symbol)

    # Keep the richer multi-timeframe score only for sentiment/context.
    # It no longer vetoes the classic 5-minute breakout engine.
    bull_score, bull_reasons, bull_blockers = _direction_score(snap, "BULLISH")
    bear_score, bear_reasons, bear_blockers = _direction_score(snap, "BEARISH")

    classic = _classic_breakout_signal(symbol, list(candles_5m[symbol]))
    classic_signal = classic.get("signal") if classic.get("status") == "SIGNAL" else None

    if classic_signal:
        direction = classic_signal["direction"]
        score = int(classic_signal["score"])
        # The old BaselineSignalEngine had no min_score attribute/veto.
        # Therefore every confirmed trend-aligned breakout remains actionable.
        grade = "A+" if score >= 80 else ("STRONG" if score >= 70 else "BREAKOUT")
        actionable = True
        blockers = []
        reasons = [
            "5M trend-aligned breakout",
            f"Fast-5 {'>' if classic_signal['side'] == 'BUY' else '<'} Slow-20",
            f"Breakout strength {classic_signal['breakout_strength']}/20",
            f"Trend strength {classic_signal['trend_strength']}/20",
            "Minimum R:R 1:1.85",
        ]
    else:
        # No classic breakout right now. Directional scores are still shown so
        # sentiment/technical cards remain informative without fabricating a call.
        if bull_score >= bear_score:
            direction = "CALL"
            score = bull_score
            reasons = bull_reasons
        else:
            direction = "PUT"
            score = bear_score
            reasons = bear_reasons

        if classic.get("reason") == "INSUFFICIENT_HISTORY":
            grade = "WARMING_UP"
            blockers = [f"5m classic warm-up {classic.get('have', 0)}/{classic.get('need', 22)}"]
        else:
            grade = "NO_TRADE"
            blockers = []
        actionable = False

    signal = {
        "symbol": symbol,
        "direction": direction,
        "score": score,
        "technical_scores": {
            "bullish": bull_score,
            "bearish": bear_score,
        },
        "technical_bias": max(0, min(100, round(50 + (bull_score - bear_score) / 2))),
        "grade": grade,
        "actionable": actionable,
        "underlying_ltp": latest.get(symbol, {}).get("ltp"),
        "reasons": reasons,
        "blockers": blockers,
        "warnings": [],
        "indicators": snap,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "CLASSIC_BREAKOUT_SIGNAL_ONLY",
        "engine": "classic_breakout_v1",
        "classic_engine": classic,
        "readiness": {
            "minimum_ready": classic.get("reason") != "INSUFFICIENT_HISTORY",
            "classic_5m_count": len(candles_5m[symbol]),
            "classic_5m_need": 22,
            "one_minute_count": snap["one_minute"]["count"],
            "five_minute_count": snap["five_minute"]["count"],
            "fifteen_minute_count": snap["fifteen_minute"]["count"],
            "daily_levels_ready": bool((snap.get("daily_levels") or {}).get("ready")),
            "five_minute_levels_ready": bool((snap.get("five_minute_levels") or {}).get("ready")),
            "note": "Primary signal engine: classic 5M trend-aligned 20-bar breakout, min R:R 1:1.85.",
        },
        "option_contract": None,
        "option_ltp": None,
        "entry": None,
        "stop_loss": None,
        "target_1": None,
        "target_2": None,
    }

    if classic_signal:
        signal["underlying_entry"] = classic_signal["entry"]
        signal["underlying_stop_loss"] = classic_signal["stop_loss"]
        signal["underlying_target_1"] = classic_signal["target_1"]
        signal["underlying_target_2"] = classic_signal["target_2"]
        signal["rr"] = classic_signal["rr"]

    technical_actionable = bool(actionable)
    signal = await _attach_auto_option_to_signal(signal)

    signal_diagnostics["evaluations"] += 1
    signal_diagnostics["last_evaluation_at"] = datetime.now(timezone.utc).isoformat()
    if technical_actionable:
        signal_diagnostics["technical_actionable"] += 1
    final_grade = str(signal.get("grade") or "")
    if signal.get("actionable"):
        signal_diagnostics["final_actionable"] += 1
    elif final_grade == "WARMING_UP":
        signal_diagnostics["warming_up"] += 1
    elif final_grade == "WATCH":
        signal_diagnostics["watch"] += 1
    elif final_grade == "NO_TRADE":
        signal_diagnostics["no_trade"] += 1

    signal_diagnostics["last_by_symbol"][symbol] = {
        "at": signal.get("generated_at"),
        "engine": signal.get("engine"),
        "direction": signal.get("direction"),
        "score": signal.get("score"),
        "technical_scores": signal.get("technical_scores"),
        "technical_bias": signal.get("technical_bias"),
        "grade": signal.get("grade"),
        "actionable": bool(signal.get("actionable")),
        "blockers": list(signal.get("blockers") or []),
        "warnings": list(signal.get("warnings") or []),
        "option_status": signal.get("option_status"),
        "option_quality_score": signal.get("option_quality_score"),
    }
    print(
        f"[CLASSIC_SIGNAL_EVAL] {symbol} direction={signal.get('direction')} "
        f"score={signal.get('score')} grade={signal.get('grade')} "
        f"actionable={bool(signal.get('actionable'))} "
        f"classic={classic.get('status')} reason={classic.get('reason')} "
        f"option_status={signal.get('option_status')} "
        f"warnings={signal.get('warnings') or []}",
        flush=True,
    )

    _spawn_background(_maybe_send_telegram_signal(dict(signal)))

    previous = signals.get(symbol)
    signals[symbol] = signal

    await broadcast({
        "type": "signal_update",
        "data": signal,
    })

    if (
        previous is None
        or previous.get("grade") != signal.get("grade")
        or previous.get("direction") != signal.get("direction")
    ):
        signal_history.appendleft(signal)
        await broadcast({
            "type": "signal_event",
            "data": signal,
        })


async def consume_signal_tick(symbol, price, received_at):
    if symbol not in SIGNAL_SYMBOLS:
        return

    if not scanner_state.get("index_scan_enabled", True):
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
        (15, active_15m, candles_15m),
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
        await save_candle_state()
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
# OPTION / MARKET INTELLIGENCE MODULE
# =========================================================

def _normalise_rows(response):
    """
    Kotak SDK responses can be a list directly, or a dict containing data.
    Return a list without inventing data.
    """
    if response is None:
        return []

    if isinstance(response, list):
        return response

    if isinstance(response, dict):
        data = response.get("data")

        if isinstance(data, list):
            return data

        # Some SDK methods may return a single row.
        if isinstance(data, dict):
            return [data]

    return []


def _depth_side_totals(rows):
    qty = 0.0
    orders = 0.0

    if not isinstance(rows, list):
        return qty, orders

    for row in rows:
        if not isinstance(row, dict):
            continue

        qty += number(row.get("quantity")) or 0.0
        orders += number(row.get("orders")) or 0.0

    return qty, orders


def _best_depth_price(rows):
    if not isinstance(rows, list) or not rows:
        return None

    first = rows[0]

    if not isinstance(first, dict):
        return None

    return number(first.get("price"))


def _analyse_option_quote(row: dict[str, Any]):
    """
    Convert a real Kotak quote row into option intelligence.
    Uses only fields documented by Kotak Quotes API:
    LTP, volume, OI, total buy/sell, OHLC and 5-level depth.
    """
    depth = row.get("depth") or {}

    buys = depth.get("buy") or []
    sells = depth.get("sell") or []

    buy_depth_qty, buy_orders = _depth_side_totals(buys)
    sell_depth_qty, sell_orders = _depth_side_totals(sells)

    best_bid = _best_depth_price(buys)
    best_ask = _best_depth_price(sells)

    spread = None
    spread_pct = None

    if (
        best_bid is not None
        and best_ask is not None
        and best_ask >= best_bid
    ):
        spread = best_ask - best_bid

        mid = (best_bid + best_ask) / 2.0

        if mid > 0:
            spread_pct = (spread / mid) * 100.0

    total_depth = buy_depth_qty + sell_depth_qty

    depth_imbalance = None

    if total_depth > 0:
        depth_imbalance = (
            (buy_depth_qty - sell_depth_qty)
            / total_depth
        ) * 100.0

    total_buy = number(row.get("total_buy"))
    total_sell = number(row.get("total_sell"))

    book_imbalance = None

    if (
        total_buy is not None
        and total_sell is not None
        and (total_buy + total_sell) > 0
    ):
        book_imbalance = (
            (total_buy - total_sell)
            / (total_buy + total_sell)
        ) * 100.0

    ltp = (
        number(row.get("ltp"))
        or number(row.get("last_traded_price"))
        or number(row.get("lp"))
    )

    oi = (
        number(row.get("open_int"))
        or number(row.get("oi"))
        or number(row.get("open_interest"))
    )

    volume = (
        number(row.get("last_volume"))
        or number(row.get("volume"))
        or number(row.get("vol"))
    )

    liquidity_score = 0
    liquidity_reasons = []

    if ltp is not None and ltp > 0:
        liquidity_score += 20
        liquidity_reasons.append("valid LTP")

    if volume is not None and volume > 0:
        liquidity_score += 20
        liquidity_reasons.append("traded volume")

    if oi is not None and oi > 0:
        liquidity_score += 20
        liquidity_reasons.append("open interest")

    if best_bid is not None and best_ask is not None:
        liquidity_score += 20
        liquidity_reasons.append("two-sided market")

    if spread_pct is not None:
        if spread_pct <= 1.0:
            liquidity_score += 20
            liquidity_reasons.append("tight spread")
        elif spread_pct <= 2.0:
            liquidity_score += 10
            liquidity_reasons.append("acceptable spread")

    if depth_imbalance is None:
        order_flow_bias = "UNKNOWN"
    elif depth_imbalance >= 15:
        order_flow_bias = "BUYING"
    elif depth_imbalance <= -15:
        order_flow_bias = "SELLING"
    else:
        order_flow_bias = "BALANCED"

    return {
        "exchange_token":
            str(
                row.get("exchange_token")
                or row.get("instrument_token")
                or ""
            ),
        "display_symbol":
            row.get("display_symbol")
            or row.get("trading_symbol")
            or "",
        "exchange":
            row.get("exchange") or "",
        "ltp": ltp,
        "change": number(row.get("change")),
        "percent_change":
            number(
                row.get("per_change")
                or row.get("percentage_change")
            ),
        "volume": volume,
        "open_interest": oi,
        "total_buy": total_buy,
        "total_sell": total_sell,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "spread_pct": spread_pct,
        "depth_buy_qty": buy_depth_qty,
        "depth_sell_qty": sell_depth_qty,
        "depth_buy_orders": buy_orders,
        "depth_sell_orders": sell_orders,
        "depth_imbalance_pct": depth_imbalance,
        "book_imbalance_pct": book_imbalance,
        "order_flow_bias": order_flow_bias,
        "liquidity_score": liquidity_score,
        "liquidity_reasons": liquidity_reasons,
        "ohlc": row.get("ohlc"),
        "depth": depth,
        "greeks": {
            "ready": False,
            "reason": (
                "Kotak Quotes API documents price/OI/depth but not "
                "option Greeks. Greeks will be calculated only after "
                "expiry/strike/underlying/IV inputs are wired."
            ),
        },
        "vix": {
            "ready": False,
            "reason": (
                "India VIX is intentionally not guessed in this module. "
                "It will be wired as a dedicated market-data input."
            ),
        },
        "source": "Kotak Neo Quotes",
        "quote_types": list(row.get("_kingbro_quote_types") or []),
        "quote_notes": list(row.get("_kingbro_quote_notes") or []),
        "mock_data": False,
        "received_at":
            datetime.now(timezone.utc).isoformat(),
    }


def _quote_once_sync(
    instrument_token: str,
    exchange_segment: str,
    quote_type: str,
):
    """Fetch one Kotak quote type and return (row, raw_response)."""
    global neo_client

    if neo_client is None:
        raise RuntimeError(
            "Kotak is not authenticated. Connect with TOTP first."
        )

    response = neo_client.quotes(
        instrument_tokens=[
            {
                "instrument_token": str(instrument_token),
                "exchange_segment": str(exchange_segment),
            }
        ],
        quote_type=quote_type,
    )

    rows = _normalise_rows(response)
    if rows and isinstance(rows[0], dict):
        return rows[0], response
    return None, response


def _merge_quote_row(base: dict[str, Any], extra: Optional[dict[str, Any]]):
    """
    Merge quote-type fallbacks into the `all` quote without inventing values.
    Non-empty fallback fields fill missing/empty fields; nested depth is merged.
    """
    merged = dict(base or {})
    if not isinstance(extra, dict):
        return merged

    for key, value in extra.items():
        if value in (None, "", [], {}):
            continue

        if key == "depth" and isinstance(value, dict):
            depth = dict(merged.get("depth") or {})
            for side, rows in value.items():
                if rows not in (None, "", [], {}):
                    depth[side] = rows
            merged["depth"] = depth
            continue

        current = merged.get(key)
        if current in (None, "", [], {}) or key in {
            "ltp", "last_traded_price", "lp",
            "open_int", "oi", "open_interest",
        }:
            merged[key] = value

    return merged


def _row_has_usable_ltp(row: dict[str, Any]) -> bool:
    return bool(
        number(row.get("ltp"))
        or number(row.get("last_traded_price"))
        or number(row.get("lp"))
    )


def _row_has_usable_oi(row: dict[str, Any]) -> bool:
    oi = (
        number(row.get("open_int"))
        or number(row.get("oi"))
        or number(row.get("open_interest"))
    )
    return oi is not None and oi > 0


def _row_has_two_sided_depth(row: dict[str, Any]) -> bool:
    depth = row.get("depth") or {}
    return bool(depth.get("buy")) and bool(depth.get("sell"))


def _quotes_sync(
    instrument_token: str,
    exchange_segment: str,
):
    """
    V7.6 quote reliability:
    - Prefer Kotak `all` exactly as before.
    - If `all` is HTTP-successful but omits a usable option LTP, retry `ltp`.
    - If derivative OI is absent, retry `oi`.
    - If depth is absent, retry documented `market_depth` and finally the
      legacy SDK spelling `depth` when supported.

    The existing LTP/OI/liquidity >= 60 signal filter is NOT changed. This
    only prevents a technically strong setup from being discarded because a
    single `all` payload was incomplete.
    """
    global neo_client

    if neo_client is None:
        raise RuntimeError(
            "Kotak is not authenticated. Connect with TOTP first."
        )

    merged: dict[str, Any] = {}
    raw_messages = []
    used_types = []

    # Primary complete quote.
    try:
        row, response = _quote_once_sync(
            instrument_token,
            exchange_segment,
            "all",
        )
        if row:
            merged = _merge_quote_row(merged, row)
            used_types.append("all")
        else:
            raw_messages.append(f"all:{safe_api_message(response)}")
    except Exception as exc:
        raw_messages.append(f"all:{type(exc).__name__}:{exc}")

    # Exact LTP fallback -- the critical missed-signal case seen in V7.5.
    if not _row_has_usable_ltp(merged):
        try:
            row, response = _quote_once_sync(
                instrument_token,
                exchange_segment,
                "ltp",
            )
            if row:
                merged = _merge_quote_row(merged, row)
                used_types.append("ltp")
            else:
                raw_messages.append(f"ltp:{safe_api_message(response)}")
        except Exception as exc:
            raw_messages.append(f"ltp:{type(exc).__name__}:{exc}")

    # Preserve the existing OI confirmation rule by fetching OI separately
    # only when the complete quote did not contain it.
    if str(exchange_segment).lower().endswith("_fo") and not _row_has_usable_oi(merged):
        try:
            row, response = _quote_once_sync(
                instrument_token,
                exchange_segment,
                "oi",
            )
            if row:
                merged = _merge_quote_row(merged, row)
                used_types.append("oi")
            else:
                raw_messages.append(f"oi:{safe_api_message(response)}")
        except Exception as exc:
            raw_messages.append(f"oi:{type(exc).__name__}:{exc}")

    # Depth is part of the existing liquidity score. Fetch it only if needed.
    if not _row_has_two_sided_depth(merged):
        for quote_type in ("market_depth", "depth"):
            try:
                row, response = _quote_once_sync(
                    instrument_token,
                    exchange_segment,
                    quote_type,
                )
                if row:
                    merged = _merge_quote_row(merged, row)
                    used_types.append(quote_type)
                else:
                    raw_messages.append(
                        f"{quote_type}:{safe_api_message(response)}"
                    )
                if _row_has_two_sided_depth(merged):
                    break
            except Exception as exc:
                raw_messages.append(
                    f"{quote_type}:{type(exc).__name__}:{exc}"
                )

    if not merged:
        detail = "; ".join(raw_messages[-4:]) or "no instrument data"
        raise RuntimeError(
            "Kotak Quotes returned no instrument data after fallbacks: "
            + detail
        )

    merged["_kingbro_quote_types"] = used_types
    if raw_messages:
        merged["_kingbro_quote_notes"] = raw_messages[-6:]
    return merged


def _search_option_sync(
    exchange_segment: str,
    symbol: str,
    expiry: str = "",
    option_type: str = "",
    strike_price: str = "",
):
    # Prefer the authenticated session so post-login baseUrl/session
    # information is preserved consistently.
    global neo_client

    client = neo_client

    if client is None:
        raise RuntimeError(
            "Kotak is not authenticated. Connect with TOTP first."
        )

    # Kotak search_scrip documents exchange_segment as mandatory and
    # symbol/expiry/option_type/strike_price as optional filters.
    response = client.search_scrip(
        exchange_segment=exchange_segment,
        symbol=symbol or "",
        expiry=expiry or "",
        option_type=option_type or "",
        strike_price=strike_price or "",
    )

    rows = _normalise_rows(response)

    # Some SDK versions return a raw list directly.
    if isinstance(response, list):
        rows = response

    return rows


def _field(row, *names):
    for name in names:
        if name in row and row.get(name) not in (None, ""):
            return row.get(name)
    return None


def _clean_upper(value):
    return re.sub(r"[^A-Z0-9]+", "", str(value or "").upper())


def _parse_expiry(value):
    """
    Parse the expiry formats commonly returned by Kotak search_scrip.
    Returns a date or None. We never invent an expiry.
    """
    raw = str(value or "").strip()
    if not raw:
        return None

    # ISO-like values sometimes include a time component.
    iso_candidate = raw[:10]
    try:
        return date.fromisoformat(iso_candidate)
    except Exception:
        pass

    compact = re.sub(r"[^A-Za-z0-9]", "", raw).upper()

    formats = (
        "%d%b%Y",   # 27AUG2026
        "%d%b%y",   # 27AUG26
        "%d%m%Y",   # 27082026
        "%Y%m%d",   # 20260827
    )

    for fmt in formats:
        try:
            return datetime.strptime(compact, fmt).date()
        except Exception:
            continue

    return None


def _strike_from_trading_symbol(trading_symbol: str):
    """
    Kotak scrip-search can expose dStrikePrice in scaled integer units
    (for example 1785000 while the trading symbol says 17850CE).
    The trading symbol is therefore the safest display-strike source.
    """
    ts = str(trading_symbol or "").upper().strip()

    match = re.search(r"(\d+(?:\.\d+)?)(CE|PE)$", ts)

    if not match:
        return None

    try:
        return float(match.group(1))
    except Exception:
        return None


def _normalise_contract(row: dict[str, Any]):
    token = _field(
        row,
        "pSymbol",
        "token",
        "instrument_token",
        "exchange_token",
    )

    expiry = _field(
        row,
        "pExpiryDate",
        "expiry",
        "expiry_date",
    )

    option_type = str(
        _field(
            row,
            "pOptionType",
            "option_type",
            "optionType",
        )
        or ""
    ).upper().strip()

    trading_symbol = str(
        _field(
            row,
            "pTrdSymbol",
            "trading_symbol",
            "display_symbol",
        )
        or ""
    ).strip()

    symbol_name = str(
        _field(
            row,
            "pSymbolName",
            "symbol",
            "symbol_name",
        )
        or ""
    ).strip()

    instrument_type = str(
        _field(
            row,
            "pInstType",
            "instrument_type",
            "instrumentType",
            "inst_type",
        )
        or ""
    ).upper().strip()

    raw_strike = number(
        _field(
            row,
            "dStrikePrice;",
            "dStrikePrice",
            "strike_price",
            "strikePrice",
        )
    )

    parsed_strike = _strike_from_trading_symbol(
        trading_symbol
    )

    # Prefer the human-readable strike encoded in the official trading symbol.
    # Preserve raw_strike separately for debugging.
    strike = (
        parsed_strike
        if parsed_strike is not None
        else raw_strike
    )

    return {
        "instrument_token": str(token or "").strip(),
        "symbol": symbol_name,
        "trading_symbol": trading_symbol,
        "expiry": str(expiry or "").strip(),
        "expiry_date": _parse_expiry(expiry),
        "option_type": option_type,
        "strike_price": strike,
        "raw_strike_price": raw_strike,
        "instrument_type": instrument_type,
        "raw": row,
    }


def _is_exact_underlying(item, wanted_symbol: str):
    """
    Prevent broad search results such as NIFTYFPI from entering the option
    selector. Prefer pSymbolName when Kotak supplies it; otherwise use a
    conservative trading-symbol prefix check.
    """
    wanted = _clean_upper(wanted_symbol)
    symbol_name = _clean_upper(item.get("symbol"))
    trading_symbol = _clean_upper(item.get("trading_symbol"))

    if symbol_name:
        return symbol_name == wanted

    if not trading_symbol.startswith(wanted):
        return False

    # The first character after the underlying should normally begin the
    # expiry encoding, not another alphabetic product suffix such as FPI.
    tail = trading_symbol[len(wanted):]
    return bool(tail) and tail[0].isdigit()


def _atm_candidates(
    rows,
    underlying_ltp: float,
    direction: str,
    wanted_symbol: str,
):
    wanted_type = "CE" if direction == "CALL" else "PE"
    today = datetime.now(timezone.utc).date()

    contracts = []

    for row in rows:
        if not isinstance(row, dict):
            continue

        item = _normalise_contract(row)

        if not _is_exact_underlying(item, wanted_symbol):
            continue

        # When Kotak supplies instrument type, require an index option.
        inst = _clean_upper(item.get("instrument_type"))
        if inst and inst not in {"OPTIDX", "IO"}:
            continue

        if item["option_type"] != wanted_type:
            continue

        if not item["instrument_token"]:
            continue

        if item["strike_price"] is None:
            continue

        expiry_date = item.get("expiry_date")
        if expiry_date is None or expiry_date < today:
            continue

        contracts.append(item)

    if not contracts:
        return []

    # Critical V5.3 rule:
    # 1) nearest valid future expiry
    # 2) only that expiry
    # 3) nearest strike to the live underlying
    nearest_expiry = min(item["expiry_date"] for item in contracts)

    contracts = [
        item for item in contracts
        if item["expiry_date"] == nearest_expiry
    ]

    contracts.sort(
        key=lambda item: abs(
            float(item["strike_price"]) - float(underlying_ltp)
        )
    )

    return contracts


async def auto_discover_option(symbol: str, direction: str):
    """
    V5.3 automatic option discovery:
    exact underlying -> index option -> CE/PE -> valid nearest expiry ->
    nearest ATM strike -> real Kotak quote -> liquidity tie-break.
    """
    ltp = latest.get(symbol, {}).get("ltp")

    if ltp is None:
        raise RuntimeError(
            f"No live underlying LTP available for {symbol}."
        )

    if symbol == "NIFTY 50":
        exchange_segment = "nse_fo"
        search_symbol = "NIFTY"
    elif symbol == "SENSEX":
        exchange_segment = "bse_fo"
        search_symbol = "SENSEX"
    else:
        raise RuntimeError("Unsupported underlying.")

    rows = await asyncio.to_thread(
        _search_option_sync,
        exchange_segment,
        search_symbol,
        "",
        "",
        "",
    )

    candidates = _atm_candidates(
        rows,
        float(ltp),
        direction,
        search_symbol,
    )

    if not candidates:
        return {
            "ready": False,
            "symbol": symbol,
            "direction": direction,
            "underlying_ltp": ltp,
            "exchange_segment": exchange_segment,
            "search_symbol": search_symbol,
            "search_rows": len(rows),
            "reason": (
                "Kotak search returned rows, but no exact-underlying "
                "index-option contract with a parseable current/future "
                "expiry matched the requested CE/PE side."
            ),
        }

    nearest_expiry = candidates[0]["expiry"]

    # Check a small ATM neighbourhood only; this avoids thousands of quote calls.
    inspected = []

    for candidate in candidates[:8]:
        public_candidate = {
            k: v
            for k, v in candidate.items()
            if k not in {"raw", "expiry_date"}
        }

        try:
            q = await inspect_option_contract(
                candidate["instrument_token"],
                exchange_segment,
            )

            inspected.append({
                **public_candidate,
                "quote": q,
            })

        except Exception as exc:
            inspected.append({
                **public_candidate,
                "quote_error": f"{type(exc).__name__}: {exc}",
            })

    usable = [
        item
        for item in inspected
        if isinstance(item.get("quote"), dict)
        and item["quote"].get("ltp") is not None
        and float(item["quote"]["ltp"]) > 0
    ]

    if not usable:
        return {
            "ready": False,
            "symbol": symbol,
            "direction": direction,
            "underlying_ltp": ltp,
            "exchange_segment": exchange_segment,
            "search_symbol": search_symbol,
            "search_rows": len(rows),
            "nearest_expiry": nearest_expiry,
            "atm_candidates": len(candidates),
            "candidates_checked": inspected,
            "reason": (
                "Exact-underlying nearest-expiry candidates were found, "
                "but Kotak Quotes (including LTP fallback) returned no usable live option LTP."
            ),
        }

    usable.sort(
        key=lambda item: (
            abs(float(item["strike_price"]) - float(ltp)),
            -(item["quote"].get("liquidity_score") or 0),
        )
    )

    selected = usable[0]

    return {
        "ready": True,
        "symbol": symbol,
        "direction": direction,
        "underlying_ltp": ltp,
        "exchange_segment": exchange_segment,
        "search_symbol": search_symbol,
        "search_rows": len(rows),
        "nearest_expiry": nearest_expiry,
        "selected": selected,
        "candidates_checked": inspected,
        "source": "Kotak Neo Scrip Search + Quotes",
        "mock_data": False,
        "selector_version": "5.4",
    }


async def inspect_option_contract(
    instrument_token: str,
    exchange_segment: str,
):
    row = await asyncio.to_thread(
        _quotes_sync,
        instrument_token,
        exchange_segment,
    )

    analysis = _analyse_option_quote(row)

    key = (
        f"{exchange_segment}|"
        f"{instrument_token}"
    )

    option_intelligence[key] = analysis

    return analysis


# =========================================================
# RENDER / FEED RELIABILITY — V7.4
# =========================================================
# Reliability supervisor retained. V7.7 restores the classic breakout
# signal engine; this section remains infrastructure-only.


def _market_session_active(now_ist: Optional[datetime] = None) -> bool:
    """True only while NSE/BSE normal cash/index market ticks are expected."""
    now_ist = now_ist or datetime.now(IST)
    if now_ist.weekday() >= 5:
        return False
    current = now_ist.time().replace(tzinfo=None)
    return dt_time(9, 15) <= current <= dt_time(15, 30)


def _keepalive_window_active(now_ist: Optional[datetime] = None) -> bool:
    """Render wake/login buffer; deliberately wider than the feed watchdog."""
    now_ist = now_ist or datetime.now(IST)
    if now_ist.weekday() >= 5:
        return False
    current = now_ist.time().replace(tzinfo=None)
    return dt_time(9, 0) <= current <= dt_time(15, 40)


def _seconds_since_iso(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds())
    except Exception:
        return None


def _looks_like_auth_expired(exc: Exception) -> bool:
    text = f"{type(exc).__name__}: {exc}".lower()
    markers = (
        "401",
        "403",
        "unauthorized",
        "invalid token",
        "token expired",
        "session expired",
        "authentication failed",
        "not authenticated",
    )
    return any(marker in text for marker in markers)


def _keepalive_url() -> str:
    base = (
        settings.KINGBRO_KEEPALIVE_URL
        or settings.RENDER_EXTERNAL_URL
        or os.getenv("RENDER_EXTERNAL_URL", "")
    ).strip()
    if not base:
        return ""
    return base.rstrip("/") + "/health"


def _keepalive_get_sync(url: str):
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "User-Agent": "KING-BRO-Render-MarketHours-Keepalive/7.4",
            "Cache-Control": "no-cache",
        },
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        return int(getattr(response, "status", 200))


async def _restart_index_feed(reason: str):
    global feed_task

    if neo_client is None or not status.get("broker_connected"):
        return False

    now = datetime.now(timezone.utc)
    last_restart = _seconds_since_iso(runtime_state.get("last_feed_restart_at"))
    cooldown = max(15, int(settings.KINGBRO_FEED_RESTART_COOLDOWN_SECONDS))
    if last_restart is not None and last_restart < cooldown:
        return False

    old_task = feed_task
    if old_task and not old_task.done():
        old_task.cancel()
        try:
            await old_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    await set_status(feed_connected=False, last_error=f"Feed restarting: {reason}")
    runtime_state["feed_restart_count"] += 1
    runtime_state["last_feed_restart_at"] = now.isoformat()
    runtime_state["last_feed_restart_reason"] = reason
    print(f"[FEED_RESTART] reason={reason}", flush=True)
    feed_task = asyncio.create_task(feed_loop())
    return True


async def _runtime_supervisor_loop():
    runtime_state["supervisor_running"] = True
    try:
        while True:
            await asyncio.sleep(15)

            if neo_client is None or not status.get("broker_connected"):
                continue
            if not _market_session_active():
                continue

            age = _seconds_since_iso(status.get("last_tick_at"))
            runtime_state["last_feed_stale_age_seconds"] = None if age is None else round(age, 1)

            stale_after = max(45, int(settings.KINGBRO_FEED_STALE_SECONDS))
            # feed_loop already handles explicit disconnects. The supervisor
            # intervenes only when a websocket claims to be connected but no
            # ticks arrive (silent/stale connection).
            if status.get("feed_connected"):
                if age is not None and age > stale_after:
                    await _restart_index_feed(f"no index tick for {int(age)}s")
                elif age is None:
                    connected_age = _seconds_since_iso(runtime_state.get("feed_connected_at"))
                    if connected_age is not None and connected_age > stale_after:
                        await _restart_index_feed(f"connected but no first tick for {int(connected_age)}s")

            # If the feed task ended unexpectedly while the authenticated
            # broker session is still available, bring it back automatically.
            if feed_task is None or feed_task.done():
                await _restart_index_feed("feed task not running")
    except asyncio.CancelledError:
        return
    finally:
        runtime_state["supervisor_running"] = False


async def _render_keepalive_loop():
    runtime_state["keepalive_running"] = True
    try:
        while True:
            interval = max(180, int(settings.KINGBRO_KEEPALIVE_INTERVAL_SECONDS))
            await asyncio.sleep(interval)

            if not settings.KINGBRO_KEEPALIVE_ENABLED:
                continue
            # Keep the Render process awake during Indian market hours even if
            # a platform restart has cleared the in-memory Kotak login. This
            # cannot recreate 2FA, but it prevents a restarted service from
            # immediately becoming idle again while the user needs to relogin.
            if not _keepalive_window_active():
                continue

            url = _keepalive_url()
            runtime_state["keepalive_url_detected"] = bool(url)
            if not url:
                runtime_state["last_keepalive_error"] = "RENDER_EXTERNAL_URL unavailable"
                continue

            try:
                code = await asyncio.to_thread(_keepalive_get_sync, url)
                runtime_state["last_keepalive_at"] = datetime.now(timezone.utc).isoformat()
                runtime_state["last_keepalive_error"] = None if code < 400 else f"HTTP {code}"
            except Exception as exc:
                runtime_state["last_keepalive_error"] = f"{type(exc).__name__}: {exc}"
                print(f"[KEEPALIVE_FAILED] {type(exc).__name__}: {exc}", flush=True)
    except asyncio.CancelledError:
        return
    finally:
        runtime_state["keepalive_running"] = False


def _start_runtime_supervisors():
    global runtime_supervisor_task, render_keepalive_task
    if runtime_supervisor_task is None or runtime_supervisor_task.done():
        runtime_supervisor_task = asyncio.create_task(_runtime_supervisor_loop())
    if render_keepalive_task is None or render_keepalive_task.done():
        render_keepalive_task = asyncio.create_task(_render_keepalive_loop())


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


                runtime_state["feed_connected_at"] = datetime.now(timezone.utc).isoformat()
                await set_status(
                    feed_connected=True,
                    last_error=None,
                )
                backoff = 2


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


            # A clean websocket close is still a disconnect. Keep the
            # authenticated broker session and reconnect automatically.
            runtime_state["feed_connected_at"] = None
            await set_status(
                feed_connected=False,
                last_error="Kotak index websocket closed; reconnecting.",
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


        except asyncio.CancelledError:
            return


        except Exception as exc:
            runtime_state["feed_connected_at"] = None
            auth_expired = _looks_like_auth_expired(exc)

            if auth_expired:
                # Only a real broker-auth failure should force a new TOTP.
                runtime_state["auth_relogin_required"] = True
                neo_client = None
                await set_status(
                    broker_connected=False,
                    feed_connected=False,
                    last_error=f"KOTAK RELOGIN REQUIRED: {type(exc).__name__}: {exc}",
                )
                print(f"[KOTAK_AUTH_EXPIRED] {type(exc).__name__}: {exc}", flush=True)
                return

            await set_status(
                feed_connected=False,
                last_error=f"{type(exc).__name__}: {exc}; reconnecting.",
            )

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


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
            "fifteen_minute_candles": {
                s: len(candles_15m[s])
                for s in SIGNAL_SYMBOLS
            },
            "scanner_state": scanner_state,
            "stock_universe_count": len(STOCK_UNIVERSE),
            "stock_signal_count": len(stock_signals),
        },
        "execution": execution_state,
        "persistence": persistence_status,
        "runtime": {
            **runtime_state,
            "market_hours_active": _market_session_active(),
            "market_session_active": _market_session_active(),
            "keepalive_window_active": _keepalive_window_active(),
            "feed_task_running": bool(feed_task and not feed_task.done()),
            "keepalive_url_detected": bool(_keepalive_url()),
            "last_tick_age_seconds": _seconds_since_iso(status.get("last_tick_at")),
        },
        "signal_diagnostics": signal_diagnostics,
        "telegram": {
            **telegram_state,
            "bot_token_configured": bool(TELEGRAM_BOT_TOKEN),
            "chat_id_configured": bool(TELEGRAM_CHAT_ID),
        },
    }



@app.get("/api/levels/{symbol}")
async def market_levels(symbol: str):
    key = symbol.strip().upper().replace("_", " ")

    aliases = {
        "NIFTY": "NIFTY 50",
        "NIFTY50": "NIFTY 50",
        "NIFTY 50": "NIFTY 50",
        "SENSEX": "SENSEX",
    }

    resolved = aliases.get(key)

    if resolved not in SIGNAL_SYMBOLS:
        raise HTTPException(
            status_code=404,
            detail="Supported symbols: NIFTY or SENSEX.",
        )

    snap = _indicator_snapshot(resolved)

    return {
        "symbol": resolved,
        "underlying_ltp": latest.get(resolved, {}).get("ltp"),
        "daily_levels": snap["daily_levels"],
        "daily_context": snap["daily_level_context"],
        "five_minute_levels": snap["five_minute_levels"],
        "five_minute_context": snap["five_minute_level_context"],
        "mock_data": False,
    }


# =========================================================
# STATE DIAGNOSTICS — V6.2
# =========================================================

@app.get("/api/state")
async def state_diagnostics():
    return {
        "persistence": persistence_status,
        "signal_engine": {
            "one_minute_candles": {
                s: len(candles_1m[s])
                for s in SIGNAL_SYMBOLS
            },
            "five_minute_candles": {
                s: len(candles_5m[s])
                for s in SIGNAL_SYMBOLS
            },
            "fifteen_minute_candles": {
                s: len(candles_15m[s])
                for s in SIGNAL_SYMBOLS
            },
            "active_1m": active_1m,
            "active_5m": active_5m,
            "active_15m": active_15m,
        },
    }


@app.post("/api/state/save")
async def force_state_save():
    await save_candle_state()
    return {
        "ok": persistence_status.get("last_error") is None,
        "persistence": persistence_status,
    }


# =========================================================
# STOCK SCANNER — V7.1
# =========================================================

def _resolve_stock_sync(symbol: str):
    """
    Resolve the fixed symbol to its current NSE cash EQ token using Kotak.
    No token is hard-coded, so broker master changes do not silently break it.
    """
    global neo_client

    if neo_client is None:
        raise RuntimeError("Authenticate first.")

    response = neo_client.search_scrip(
        exchange_segment="nse_cm",
        symbol=symbol,
        expiry="",
        option_type="",
        strike_price="",
    )

    rows = _normalise_rows(response)

    wanted = str(symbol).upper().strip()

    for row in rows:
        if not isinstance(row, dict):
            continue

        trading_symbol = str(
            row.get("pTrdSymbol")
            or row.get("trading_symbol")
            or row.get("display_symbol")
            or ""
        ).upper().strip()

        symbol_name = str(
            row.get("pSymbolName")
            or row.get("symbol")
            or row.get("symbol_name")
            or ""
        ).upper().strip()

        token = (
            row.get("pSymbol")
            or row.get("instrument_token")
            or row.get("exchange_token")
        )

        inst_type = str(
            row.get("pInstType")
            or row.get("instrument_type")
            or ""
        ).upper().strip()

        # Strong preference: exact NSE equity trading symbol.
        exact_eq = trading_symbol == f"{wanted}-EQ"
        exact_name = symbol_name == wanted

        if (
            token not in (None, "")
            and (exact_eq or exact_name)
            and inst_type not in {"OPTIDX", "FUTIDX", "OPTSTK", "FUTSTK"}
        ):
            return {
                "symbol": wanted,
                "instrument_token": str(token),
                "exchange_segment": "nse_cm",
                "trading_symbol": trading_symbol or f"{wanted}-EQ",
            }

    return None


async def _resolve_stock_universe():
    resolved = {}
    unresolved = []

    # Resolve sequentially to keep load predictable on the free backend.
    for symbol in STOCK_UNIVERSE:
        try:
            item = await asyncio.to_thread(
                _resolve_stock_sync,
                symbol,
            )
            if item:
                resolved[symbol] = item
            else:
                unresolved.append(symbol)
        except Exception:
            unresolved.append(symbol)

    stock_token_map.clear()
    stock_token_map.update(resolved)

    scanner_state["stock_resolved"] = len(resolved)
    scanner_state["stock_unresolved"] = len(unresolved)

    return resolved, unresolved


def _stock_snapshot(symbol):
    one = list(stock_candles_1m[symbol])
    five = list(stock_candles_5m[symbol])
    fifteen = list(stock_candles_15m[symbol])

    c1 = [c["close"] for c in one]
    c5 = [c["close"] for c in five]
    c15 = [c["close"] for c in fifteen]

    return {
        "one": {
            "count": len(one),
            "ema9": _ema(c1, 9),
            "ema21": _ema(c1, 21),
            "rsi14": _rsi(c1, 14),
            "williams_r14": _williams_r(one, 14),
            "price_action": _price_action(one),
            "breakout": _breakout(one),
            "atr14": _atr(one, 14),
        },
        "five": {
            "count": len(five),
            "ema9": _ema(c5, 9),
            "ema21": _ema(c5, 21),
            "ma20": _sma(c5, 20),
            "rsi14": _rsi(c5, 14),
            "price_action": _price_action(five),
            "breakout": _breakout(five),
        },
        "fifteen": {
            "count": len(fifteen),
            "ema9": _ema(c15, 9),
            "ema21": _ema(c15, 21),
            "rsi14": _rsi(c15, 14),
            "price_action": _price_action(fifteen),
            "breakout": _breakout(fifteen),
        },
    }


def _score_stock_direction(snapshot, direction):
    one = snapshot["one"]
    five = snapshot["five"]
    fifteen = snapshot["fifteen"]

    score = 0
    reasons = []
    blockers = []

    # Warm-up: intentionally shorter than index option engine.
    if one["count"] < 22:
        blockers.append(f"1m warm-up {one['count']}/22")
    if five["count"] < 9:
        blockers.append(f"5m warm-up {five['count']}/9")
    if fifteen["count"] < 3:
        blockers.append(f"15m warm-up {fifteen['count']}/3")

    bullish = direction == "BUY"

    if fifteen["price_action"] == ("BULLISH" if bullish else "BEARISH"):
        score += 20
        reasons.append("15M price action")

    if five["ema9"] is not None and five["ema21"] is not None:
        ok = (
            five["ema9"] > five["ema21"]
            if bullish
            else five["ema9"] < five["ema21"]
        )
        if ok:
            score += 20
            reasons.append("5M EMA 9/21")

    if one["ema9"] is not None and one["ema21"] is not None:
        ok = (
            one["ema9"] > one["ema21"]
            if bullish
            else one["ema9"] < one["ema21"]
        )
        if ok:
            score += 15
            reasons.append("1M EMA 9/21")

    if one["breakout"] == ("BULLISH" if bullish else "BEARISH"):
        score += 20
        reasons.append("1M breakout")

    rsi = one["rsi14"]
    if rsi is not None:
        if bullish and 54 <= rsi <= 75:
            score += 10
            reasons.append("RSI bullish")
        elif (not bullish) and 25 <= rsi <= 46:
            score += 10
            reasons.append("RSI bearish")

    wr = one["williams_r14"]
    if wr is not None:
        if bullish and -55 <= wr <= -5:
            score += 10
            reasons.append("Williams %R bullish")
        elif (not bullish) and -95 <= wr <= -45:
            score += 10
            reasons.append("Williams %R bearish")

    if five["price_action"] == ("BULLISH" if bullish else "BEARISH"):
        score += 5
        reasons.append("5M price action")

    return min(score, 100), reasons, blockers


async def _evaluate_stock_signal(symbol):
    snap = _stock_snapshot(symbol)

    buy_score, buy_reasons, buy_blockers = _score_stock_direction(
        snap, "BUY"
    )
    sell_score, sell_reasons, sell_blockers = _score_stock_direction(
        snap, "SELL"
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

    ltp = stock_latest.get(symbol, {}).get("ltp")
    atr = snap["one"].get("atr14")

    entry = stop = t1 = t2 = None

    if actionable and ltp is not None:
        entry = round(float(ltp), 2)

        # ATR-based stock trade plan. If ATR is unavailable, stay non-actionable.
        if atr is None or atr <= 0:
            actionable = False
            grade = "ATR_NOT_READY"
            blockers = list(blockers) + ["ATR not ready"]
        else:
            risk = max(float(atr) * 1.2, entry * 0.003)
            if direction == "BUY":
                stop = round(entry - risk, 2)
                t1 = round(entry + risk, 2)
                t2 = round(entry + (2 * risk), 2)
            else:
                stop = round(entry + risk, 2)
                t1 = round(entry - risk, 2)
                t2 = round(entry - (2 * risk), 2)

    signal = {
        "symbol": symbol,
        "direction": direction,
        "score": score,
        "grade": grade,
        "actionable": actionable,
        "ltp": ltp,
        "entry": entry,
        "stop_loss": stop,
        "target_1": t1,
        "target_2": t2,
        "reasons": reasons,
        "blockers": blockers,
        "indicators": snap,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "STOCK_SIGNAL_ONLY",
    }

    await _maybe_send_telegram_signal(signal)
    stock_signals[symbol] = signal

    await broadcast({
        "type": "stock_signal_update",
        "data": signal,
    })


async def _consume_stock_tick(symbol, price, received_at):
    if not scanner_state.get("stock_scan_enabled"):
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
        (1, stock_active_1m, stock_candles_1m),
        (5, stock_active_5m, stock_candles_5m),
        (15, stock_active_15m, stock_candles_15m),
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

    if closed:
        await _evaluate_stock_signal(symbol)


def _stock_symbol_from_message(data):
    token = str(
        data.get("instrument_token")
        or data.get("exchange_token")
        or ""
    )

    trading_symbol = str(
        data.get("trading_symbol")
        or data.get("display_symbol")
        or ""
    ).upper()

    for symbol, item in stock_token_map.items():
        if token and token == item.get("instrument_token"):
            return symbol

        expected = str(item.get("trading_symbol") or "").upper()
        if trading_symbol and expected and trading_symbol == expected:
            return symbol

    return None


async def stock_feed_loop():
    global neo_client

    scanner_state["stock_scan_running"] = False
    scanner_state["stock_last_error"] = None

    if neo_client is None:
        scanner_state["stock_last_error"] = "Authenticate first."
        return

    resolved, unresolved = await _resolve_stock_universe()

    if not resolved:
        scanner_state["stock_last_error"] = (
            "No fixed-universe stock tokens could be resolved."
        )
        return

    tokens = [
        WsToken(
            item["exchange_segment"],
            item["instrument_token"],
        )
        for item in resolved.values()
    ]

    try:
        async with neo_client.create_websocket() as ws:
            await ws.subscribe_scrips(tokens)

            scanner_state["stock_scan_running"] = True
            scanner_state["stock_last_error"] = None

            await broadcast({
                "type": "stock_scanner_status",
                "data": {
                    "running": True,
                    "resolved": len(resolved),
                    "unresolved": unresolved,
                },
            })

            async for message in ws:
                if not scanner_state.get("stock_scan_enabled"):
                    break

                if not isinstance(message, SFeedScrip):
                    continue

                try:
                    data = message.model_dump()
                except Exception:
                    data = {}

                symbol = _stock_symbol_from_message(data)

                if symbol is None:
                    continue

                ltp = (
                    data.get("last_traded_price")
                    or data.get("ltp")
                    or data.get("last_price")
                )

                price = number(ltp)

                if price is None or price <= 0:
                    continue

                now = datetime.now(timezone.utc).isoformat()

                item = {
                    "symbol": symbol,
                    "trading_symbol": data.get("trading_symbol"),
                    "instrument_token": data.get("instrument_token"),
                    "ltp": price,
                    "change": number(
                        data.get("change")
                        or data.get("net_change")
                    ),
                    "percent_change": number(
                        data.get("percentage_change")
                        or data.get("percent_change")
                        or data.get("per_change")
                    ),
                    "received_at": now,
                }

                stock_latest[symbol] = item

                await _consume_stock_tick(
                    symbol,
                    price,
                    now,
                )

    except asyncio.CancelledError:
        pass

    except Exception as exc:
        scanner_state["stock_last_error"] = (
            f"{type(exc).__name__}: {exc}"
        )

    finally:
        scanner_state["stock_scan_running"] = False
        # Never leave the UI stuck in STARTING after the stock feed exits.
        # A dead/cancelled feed is not an enabled scanner.
        scanner_state["stock_scan_enabled"] = False

        await broadcast({
            "type": "stock_scanner_status",
            "data": {
                "running": False,
                "last_error": scanner_state["stock_last_error"],
            },
        })


def _best_stock_signals(limit=12):
    rows = list(stock_signals.values())

    rows.sort(
        key=lambda x: (
            bool(x.get("actionable")),
            float(x.get("score") or 0),
        ),
        reverse=True,
    )

    return rows[:limit]


# =========================================================
# MANUAL ORDER EXECUTION + POSITIONS — V7.2
# =========================================================

def _require_broker():
    if neo_client is None or not status.get("broker_connected"):
        raise HTTPException(
            status_code=409,
            detail="Broker session is not connected. Login with TOTP first.",
        )


def _api_rows(response):
    if response is None:
        return []

    if isinstance(response, list):
        return response

    if not isinstance(response, dict):
        return []

    data = response.get("data")

    # Some SDK responses wrap data twice.
    if isinstance(data, dict) and isinstance(data.get("data"), list):
        return data["data"]

    if isinstance(data, list):
        return data

    return []


def _int_value(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return default


def _float_value(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def _position_net_qty(row):
    # Prefer broker-provided net quantity when present.
    for key in (
        "netQty",
        "net_qty",
        "netQuantity",
        "qty",
    ):
        if row.get(key) not in (None, ""):
            return _int_value(row.get(key))

    cf_buy = _int_value(row.get("cfBuyQty"))
    fl_buy = _int_value(row.get("flBuyQty"))
    cf_sell = _int_value(row.get("cfSellQty"))
    fl_sell = _int_value(row.get("flSellQty"))

    return (cf_buy + fl_buy) - (cf_sell + fl_sell)


def _normalise_position(row):
    net_qty = _position_net_qty(row)

    trading_symbol = str(
        row.get("trdSym")
        or row.get("tradingSymbol")
        or row.get("trading_symbol")
        or row.get("sym")
        or ""
    )

    exchange_segment = str(
        row.get("exSeg")
        or row.get("exchangeSegment")
        or row.get("exchange_segment")
        or ""
    )

    product = str(
        row.get("prod")
        or row.get("product")
        or "MIS"
    )

    avg_price = _float_value(
        row.get("avgPrc")
        or row.get("averagePrice")
        or row.get("avg_price")
    )

    token = str(
        row.get("tok")
        or row.get("instrumentToken")
        or row.get("instrument_token")
        or ""
    )

    return {
        "trading_symbol": trading_symbol,
        "exchange_segment": exchange_segment,
        "product": product,
        "instrument_token": token,
        "net_quantity": net_qty,
        "side": (
            "LONG"
            if net_qty > 0
            else "SHORT"
            if net_qty < 0
            else "FLAT"
        ),
        "average_price": avg_price,
        "raw": row,
    }


async def _position_ltp(position):
    token = position.get("instrument_token")
    exchange_segment = position.get("exchange_segment")

    if not token or not exchange_segment:
        return None

    try:
        response = await asyncio.to_thread(
            neo_client.quotes,
            instrument_tokens=[{
                "instrument_token": str(token),
                "exchange_segment": str(exchange_segment),
            }],
            quote_type="all",
        )

        rows = _normalise_rows(response)

        for row in rows:
            if not isinstance(row, dict):
                continue

            value = number(
                row.get("ltp")
                or row.get("last_traded_price")
                or row.get("last_price")
            )

            if value is not None:
                return value

    except Exception:
        return None

    return None


async def _positions_payload():
    _require_broker()

    response = await asyncio.to_thread(
        neo_client.positions
    )

    rows = _api_rows(response)
    items = []

    for row in rows:
        if not isinstance(row, dict):
            continue

        item = _normalise_position(row)

        # Keep flat rows out of the live positions screen.
        if item["net_quantity"] == 0:
            continue

        ltp = await _position_ltp(item)
        item["ltp"] = ltp

        avg = item["average_price"]
        qty = item["net_quantity"]

        # UI convenience P&L. Raw broker row is also returned.
        item["unrealized_pnl"] = (
            round((ltp - avg) * qty, 2)
            if ltp is not None and avg
            else None
        )

        items.append(item)

    return {
        "count": len(items),
        "items": items,
        "raw": response,
    }


def _validate_manual_order(req):
    segment = req.exchange_segment.strip().lower()
    product = req.product.strip().upper()
    order_type = req.order_type.strip().upper()
    side = req.transaction_type.strip().upper()
    validity = req.validity.strip().upper()
    trading_symbol = req.trading_symbol.strip().upper()

    if segment not in {
        "nse_cm", "bse_cm", "nse_fo", "bse_fo", "mcx_fo"
    }:
        raise HTTPException(422, "Unsupported exchange segment.")

    if product not in {"CNC", "MIS", "NRML", "MTF"}:
        raise HTTPException(422, "Invalid product.")

    if order_type not in {"MKT", "L", "SL", "SL-M"}:
        raise HTTPException(422, "Invalid order type.")

    if side not in {"B", "S"}:
        raise HTTPException(422, "transaction_type must be B or S.")

    if validity not in {"DAY", "IOC"}:
        raise HTTPException(422, "Invalid validity.")

    if not trading_symbol:
        raise HTTPException(422, "trading_symbol is required.")

    if order_type == "L" and req.price <= 0:
        raise HTTPException(422, "Limit order requires price > 0.")

    return {
        "exchange_segment": segment,
        "product": product,
        "price": (
            str(req.price)
            if order_type == "L"
            else "0"
        ),
        "order_type": order_type,
        "quantity": str(req.quantity),
        "validity": validity,
        "trading_symbol": trading_symbol,
        "transaction_type": side,
    }


@app.get("/api/execution/status")
async def execution_status():
    return execution_state


@app.post("/api/orders/manual")
async def place_manual_order(req: ManualOrderRequest):
    _require_broker()

    if not req.confirm:
        raise HTTPException(
            status_code=400,
            detail="Manual confirmation is required before placing a live order.",
        )

    payload = _validate_manual_order(req)

    # Optional client-side idempotency key prevents accidental double-clicks.
    request_id = req.client_request_id.strip()

    if request_id and request_id in recent_manual_requests:
        return {
            "ok": True,
            "duplicate_prevented": True,
            "response": recent_manual_requests[request_id],
        }

    try:
        response = await asyncio.to_thread(
            neo_client.place_order,
            **payload,
        )

        execution_state["last_order_at"] = (
            datetime.now(timezone.utc).isoformat()
        )
        execution_state["last_order_error"] = None

        if request_id:
            recent_manual_requests[request_id] = response

            # Keep memory bounded.
            if len(recent_manual_requests) > 100:
                oldest = next(iter(recent_manual_requests))
                recent_manual_requests.pop(oldest, None)

        await broadcast({
            "type": "manual_order_update",
            "data": {
                "request": payload,
                "response": response,
            },
        })

        return {
            "ok": True,
            "manual_only": True,
            "request": payload,
            "response": response,
        }

    except Exception as exc:
        execution_state["last_order_error"] = (
            f"{type(exc).__name__}: {exc}"
        )
        raise HTTPException(
            status_code=502,
            detail=execution_state["last_order_error"],
        )


@app.get("/api/orders")
async def get_orders():
    _require_broker()

    try:
        response = await asyncio.to_thread(
            neo_client.order_report
        )
        return {
            "ok": True,
            "orders": _api_rows(response),
            "raw": response,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"{type(exc).__name__}: {exc}",
        )


@app.get("/api/positions")
async def get_positions():
    try:
        return await _positions_payload()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"{type(exc).__name__}: {exc}",
        )


@app.post("/api/positions/square-off")
async def square_off_position(req: SquareOffRequest):
    _require_broker()

    if not req.confirm:
        raise HTTPException(
            status_code=400,
            detail="Square-off confirmation is required.",
        )

    if req.current_net_quantity == 0:
        raise HTTPException(
            status_code=400,
            detail="Position is already flat.",
        )

    max_qty = abs(int(req.current_net_quantity))

    if req.quantity > max_qty:
        raise HTTPException(
            status_code=400,
            detail=f"Square-off quantity cannot exceed open quantity {max_qty}.",
        )

    side = "S" if req.current_net_quantity > 0 else "B"

    order = ManualOrderRequest(
        exchange_segment=req.exchange_segment,
        trading_symbol=req.trading_symbol,
        transaction_type=side,
        quantity=req.quantity,
        product=req.product,
        order_type="MKT",
        price=0,
        validity="DAY",
        confirm=True,
        client_request_id=(
            "SQOFF-"
            + req.trading_symbol
            + "-"
            + str(datetime.now(timezone.utc).timestamp())
        ),
    )

    return await place_manual_order(order)


@app.post("/api/positions/square-off-all")
async def square_off_all(req: SquareOffAllRequest):
    _require_broker()

    if req.confirm_text.strip().upper() != "SQUARE OFF ALL":
        raise HTTPException(
            status_code=400,
            detail='Type "SQUARE OFF ALL" exactly to confirm.',
        )

    positions = await _positions_payload()

    results = []

    # Intentionally sequential: easier to audit and avoids burst orders.
    for position in positions["items"]:
        qty = abs(int(position["net_quantity"]))

        if qty <= 0:
            continue

        side = (
            "S"
            if position["net_quantity"] > 0
            else "B"
        )

        payload = {
            "exchange_segment": position["exchange_segment"],
            "product": position["product"],
            "price": "0",
            "order_type": "MKT",
            "quantity": str(qty),
            "validity": "DAY",
            "trading_symbol": position["trading_symbol"],
            "transaction_type": side,
        }

        try:
            response = await asyncio.to_thread(
                neo_client.place_order,
                **payload,
            )
            results.append({
                "ok": True,
                "position": position["trading_symbol"],
                "quantity": qty,
                "response": response,
            })
        except Exception as exc:
            results.append({
                "ok": False,
                "position": position["trading_symbol"],
                "quantity": qty,
                "error": f"{type(exc).__name__}: {exc}",
            })

    execution_state["last_order_at"] = (
        datetime.now(timezone.utc).isoformat()
    )

    return {
        "ok": all(x["ok"] for x in results) if results else True,
        "manual_only": True,
        "count": len(results),
        "results": results,
    }


# =========================================================
# SCANNER CONTROLS — V7
# =========================================================

@app.get("/api/scanners")
async def scanners_status():
    return {
        "index": {
            "enabled": scanner_state["index_scan_enabled"],
            "symbols": list(SIGNAL_SYMBOLS),
        },
        "stocks": {
            "enabled": scanner_state["stock_scan_enabled"],
            "running": scanner_state["stock_scan_running"],
            "resolved": scanner_state["stock_resolved"],
            "unresolved": scanner_state["stock_unresolved"],
            "last_error": scanner_state["stock_last_error"],
            "universe_count": len(STOCK_UNIVERSE),
            "universe": list(STOCK_UNIVERSE),
        },
    }


@app.post("/api/scanners/index/start")
async def start_index_scanner():
    scanner_state["index_scan_enabled"] = True
    return {"ok": True, "scanner_state": scanner_state}


@app.post("/api/scanners/index/stop")
async def stop_index_scanner():
    scanner_state["index_scan_enabled"] = False
    return {"ok": True, "scanner_state": scanner_state}


@app.post("/api/scanners/stocks/start")
async def start_stock_scanner():
    global stock_feed_task

    if neo_client is None:
        raise HTTPException(
            status_code=409,
            detail="Connect/authenticate first.",
        )

    scanner_state["stock_scan_enabled"] = True
    scanner_state["stock_scan_running"] = False
    scanner_state["stock_last_error"] = None

    if stock_feed_task and not stock_feed_task.done():
        return {
            "ok": True,
            "message": "Stock scanner is already running.",
            "scanner_state": scanner_state,
        }

    stock_feed_task = asyncio.create_task(
        stock_feed_loop()
    )

    return {
        "ok": True,
        "message": (
            f"Stock scanner starting for fixed {len(STOCK_UNIVERSE)}-stock universe."
        ),
        "scanner_state": scanner_state,
        "universe": list(STOCK_UNIVERSE),
    }


@app.post("/api/scanners/stocks/stop")
async def stop_stock_scanner():
    global stock_feed_task

    scanner_state["stock_scan_enabled"] = False

    if stock_feed_task and not stock_feed_task.done():
        stock_feed_task.cancel()

    scanner_state["stock_scan_running"] = False

    return {
        "ok": True,
        "message": "Stock scanner stopped.",
        "scanner_state": scanner_state,
    }


@app.get("/api/stocks/signals")
async def get_stock_signals():
    return {
        "scanner": scanner_state,
        "universe": list(STOCK_UNIVERSE),
        "resolved_tokens": stock_token_map,
        "latest": stock_latest,
        "signals": stock_signals,
        "best": _best_stock_signals(),
    }


@app.get("/api/stocks/signals/best")
async def get_best_stock_signals(limit: int = 12):
    safe_limit = max(1, min(int(limit), 40))

    return {
        "scanner": scanner_state,
        "count": min(
            safe_limit,
            len(stock_signals),
        ),
        "items": _best_stock_signals(
            safe_limit
        ),
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

        runtime_state["auth_relogin_required"] = False
        runtime_state["restart_login_required"] = False
        runtime_state["broker_authenticated_at"] = datetime.now(timezone.utc).isoformat()
        runtime_state["feed_connected_at"] = None
        scanner_state["index_scan_enabled"] = True

        await set_status(
            broker_connected=True,
            feed_connected=False,
            last_tick_at=None,
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


@app.get("/api/signals/readiness")
async def signal_readiness():
    items = {}
    for symbol in SIGNAL_SYMBOLS:
        snap = _indicator_snapshot(symbol)
        one_count = snap["one_minute"]["count"]
        five_count = snap["five_minute"]["count"]
        fifteen_count = snap["fifteen_minute"]["count"]
        blockers = []
        if one_count < 22:
            blockers.append(f"1m {one_count}/22")
        if five_count < 3:
            blockers.append(f"5m {five_count}/3")
        items[symbol] = {
            "ready": not blockers,
            "blockers": blockers,
            "one_minute_count": one_count,
            "five_minute_count": five_count,
            "fifteen_minute_count": fifteen_count,
            "daily_levels_ready": bool((snap.get("daily_levels") or {}).get("ready")),
            "five_minute_levels_ready": bool((snap.get("five_minute_levels") or {}).get("ready")),
        }
    return {
        "mode": "FAST_SAFE_LIVE_WARMUP",
        "minimum": {
            "one_minute_completed": 22,
            "five_minute_completed": 3,
            "fifteen_minute": "optional confirmation",
        },
        "items": items,
        "mock_data": False,
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
            detail="V6 signal engine scans only NIFTY 50 and SENSEX.",
        )

    return {
        "signal": signals.get(key),
        "indicators": _indicator_snapshot(key),
        "one_minute_candles": list(candles_1m[key])[-50:],
        "five_minute_candles": list(candles_5m[key])[-50:],
        "fifteen_minute_candles": list(candles_15m[key])[-50:],
    }




# =========================================================
# OPTION / MARKET INTELLIGENCE API
# =========================================================

@app.post("/api/options/search")
async def search_option(
    body: OptionSearchRequest
):
    """
    Flexible Kotak scrip search.
    exchange_segment is mandatory; expiry/option_type/strike are optional.
    """

    option_type = body.option_type.strip().upper()

    if option_type and option_type not in {"CE", "PE"}:
        raise HTTPException(
            status_code=400,
            detail="option_type must be CE, PE, or blank.",
        )

    try:
        rows = await asyncio.to_thread(
            _search_option_sync,
            body.exchange_segment,
            body.symbol.strip().upper(),
            body.expiry.strip(),
            option_type,
            body.strike_price.strip(),
        )

        return {
            "source": "Kotak Neo Scrip Search",
            "mock_data": False,
            "filters": {
                "exchange_segment": body.exchange_segment,
                "symbol": body.symbol.strip().upper(),
                "expiry": body.expiry.strip(),
                "option_type": option_type,
                "strike_price": body.strike_price.strip(),
            },
            "count": len(rows),
            "items": rows[:100],
        }

    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "error_type": type(exc).__name__,
                "message": str(exc),
            },
        )


@app.post("/api/options/auto/{symbol}/{direction}")
async def auto_option(
    symbol: str,
    direction: str,
):
    aliases = {
        "NIFTY": "NIFTY 50",
        "NIFTY50": "NIFTY 50",
        "NIFTY 50": "NIFTY 50",
        "SENSEX": "SENSEX",
    }

    key = aliases.get(symbol.strip().upper())
    side = direction.strip().upper()

    if not key:
        raise HTTPException(
            status_code=404,
            detail="Auto option discovery supports NIFTY 50 and SENSEX.",
        )

    if side not in {"CALL", "PUT"}:
        raise HTTPException(
            status_code=400,
            detail="direction must be CALL or PUT.",
        )

    try:
        return await auto_discover_option(key, side)

    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "error_type": type(exc).__name__,
                "message": str(exc),
            },
        )


@app.post("/api/options/inspect")
async def inspect_option(
    body: OptionInspectRequest
):
    """
    Fetch one real option quote and calculate:
    LTP, OI, volume, spread, depth imbalance and liquidity score.
    """
    try:
        return await inspect_option_contract(
            body.instrument_token,
            body.exchange_segment,
        )

    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "error_type": type(exc).__name__,
                "message": str(exc),
            },
        )


@app.post("/api/signals/{symbol}/attach-option")
async def attach_option_to_signal(
    symbol: str,
    body: AttachOptionRequest,
):
    aliases = {
        "NIFTY": "NIFTY 50",
        "NIFTY50": "NIFTY 50",
        "NIFTY 50": "NIFTY 50",
        "SENSEX": "SENSEX",
    }

    key = aliases.get(
        symbol.strip().upper()
    )

    if not key:
        raise HTTPException(
            status_code=404,
            detail=(
                "Signal engine scans only "
                "NIFTY 50 and SENSEX."
            ),
        )

    if key not in signals:
        raise HTTPException(
            status_code=409,
            detail=(
                "No signal snapshot exists yet. "
                "Wait for the live candle engine."
            ),
        )

    try:
        option_data = (
            await inspect_option_contract(
                body.instrument_token,
                body.exchange_segment,
            )
        )

        signal = dict(signals[key])

        signal["option_contract"] = {
            "instrument_token":
                body.instrument_token,
            "exchange_segment":
                body.exchange_segment,
            "display_symbol":
                option_data.get(
                    "display_symbol"
                ),
        }

        signal["option_ltp"] = (
            option_data.get("ltp")
        )

        signal["option_intelligence"] = (
            option_data
        )

        # Quality confirmation is additive only.
        # It does NOT overwrite the technical score.
        option_quality_score = (
            option_data.get(
                "liquidity_score"
            )
            or 0
        )

        signal["option_quality_score"] = (
            option_quality_score
        )

        signal["option_quality_pass"] = (
            option_quality_score >= 60
        )

        signals[key] = signal

        await broadcast({
            "type":
                "signal_update",
            "data":
                signal,
        })

        return signal

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "error_type":
                    type(exc).__name__,
                "message":
                    str(exc),
            },
        )


@app.get("/api/options/cache")
async def option_cache():
    return {
        "count":
            len(option_intelligence),
        "items":
            option_intelligence,
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
