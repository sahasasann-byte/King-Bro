import asyncio
import inspect
import json
import os
import re
import urllib.parse
import urllib.request
import urllib.error
import time
from collections import deque
from datetime import datetime, timezone, date, time as dt_time, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from neo_api_client import NeoAPI
from neo_api_client.websocket.feed import WsToken, SFeedIndex, SFeedScrip

APP_VERSION = "V8.2-SMART-FIX"
IST = ZoneInfo("Asia/Kolkata")

app = FastAPI(
    title="KING BRO V7.1 Telegram 429 Safe",
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

# Reliability controls. Internal keepalive runs ONLY during the market feed window.
KEEPALIVE_ENABLED = os.getenv("KINGBRO_KEEPALIVE_ENABLED", "true").strip().lower() in (
    "1", "true", "yes", "on",
)
KEEPALIVE_INTERVAL_SECONDS = int(os.getenv("KINGBRO_KEEPALIVE_INTERVAL_SECONDS", "480") or 480)
KEEPALIVE_URL = (
    os.getenv("KINGBRO_KEEPALIVE_URL", "").strip().rstrip("/")
    or (f"{PUBLIC_URL}/health" if PUBLIC_URL else "")
)
SUPERVISOR_ENABLED = os.getenv("KINGBRO_SUPERVISOR_ENABLED", "true").strip().lower() in (
    "1", "true", "yes", "on",
)
FEED_STALE_SECONDS = int(os.getenv("KINGBRO_FEED_STALE_SECONDS", "120") or 120)
AUTO_STATE_SAVE_SECONDS = int(os.getenv("KINGBRO_AUTO_STATE_SAVE_SECONDS", "900") or 900)
STATE_SAVE_MIN_INTERVAL_SECONDS = int(os.getenv("KINGBRO_STATE_SAVE_MIN_INTERVAL_SECONDS", "240") or 240)

# Official kotakneoapi 3.0.6 exposes historical_data(). This is best-effort only:
# Gist restore remains the primary durable source, and a failed historical call never
# changes the original V7.3 strategy or blocks the live feed.
HISTORICAL_BACKFILL_ENABLED = os.getenv("KINGBRO_HISTORICAL_BACKFILL", "true").strip().lower() in (
    "1", "true", "yes", "on",
)
HISTORICAL_LOOKBACK_DAYS = int(os.getenv("KINGBRO_HISTORICAL_LOOKBACK_DAYS", "5") or 5)

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

MAX_1M = 900
MAX_5M = 240
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
    "last_state_save_error": None,
    "last_state_save_error_at": None,
    "last_state_restore_at": None,
    "last_keepalive_at": None,
    "last_keepalive_ok": None,
    "supervisor_running": False,
    "feed_restart_count": 0,
    "last_feed_restart_at": None,
    "relogin_required": False,
    "last_relogin_notice_at": None,
    "historical_backfill_attempted": False,
    "historical_backfill_loaded": 0,
    "historical_backfill_error": None,
    "evaluations": 0,
    "warming_up": 0,
    "no_trade": 0,
    "technical_actionable": 0,
    "option_filtered": 0,
    "option_errors": 0,
    "final_actionable": 0,
    "last_evaluation_at": None,
    "last_score_by_symbol": {},
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
class TelegramRateLimit(RuntimeError):
    def __init__(self, retry_after=1, detail="Telegram 429 Too Many Requests"):
        self.retry_after = max(1, int(retry_after or 1))
        super().__init__(f"{detail}; retry_after={self.retry_after}s")


telegram_send_lock = asyncio.Lock()
telegram_last_send_monotonic = 0.0
telegram_diag = {
    "last_send_ok_at": None,
    "last_send_error": None,
    "last_429_at": None,
    "last_retry_after": None,
    "webhook_setup_action": None,
    "commands_setup_action": None,
}


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

    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            raw = response.read().decode("utf-8")
            data = json.loads(raw)
    except urllib.error.HTTPError as exc:
        raw = ""
        try:
            raw = exc.read().decode("utf-8")
        except Exception:
            pass

        data = None
        if raw:
            try:
                data = json.loads(raw)
            except Exception:
                data = None

        if exc.code == 429:
            retry_after = None
            if isinstance(data, dict):
                retry_after = (
                    (data.get("parameters") or {}).get("retry_after")
                )
            if retry_after is None:
                try:
                    retry_after = int(exc.headers.get("Retry-After") or 1)
                except Exception:
                    retry_after = 1
            description = (
                data.get("description")
                if isinstance(data, dict)
                else "Telegram 429 Too Many Requests"
            )
            raise TelegramRateLimit(retry_after, description) from exc

        raise RuntimeError(
            f"Telegram HTTP {exc.code}: {raw or exc.reason}"
        ) from exc

    if not data.get("ok"):
        if int(data.get("error_code") or 0) == 429:
            retry_after = (data.get("parameters") or {}).get("retry_after") or 1
            raise TelegramRateLimit(retry_after, data.get("description") or "Telegram 429")
        raise RuntimeError(
            f"Telegram API error: {data}"
        )

    return data


async def telegram_send(message, with_keyboard=False):
    global telegram_last_send_monotonic

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

    # Serialize all sends from this process and keep a safe per-chat gap.
    # This prevents /status + startup/relogin bursts from tripping Telegram's
    # per-chat flood control.
    async with telegram_send_lock:
        elapsed = time.monotonic() - telegram_last_send_monotonic
        if elapsed < 1.20:
            await asyncio.sleep(1.20 - elapsed)

        last_error = None
        for attempt in range(1, 6):
            try:
                await asyncio.to_thread(
                    telegram_api,
                    "sendMessage",
                    payload,
                )
                telegram_last_send_monotonic = time.monotonic()
                telegram_diag["last_send_ok_at"] = datetime.now(timezone.utc).isoformat()
                telegram_diag["last_send_error"] = None
                return True
            except TelegramRateLimit as exc:
                last_error = exc
                telegram_diag["last_429_at"] = datetime.now(timezone.utc).isoformat()
                telegram_diag["last_retry_after"] = exc.retry_after
                telegram_diag["last_send_error"] = str(exc)
                wait_for = min(max(float(exc.retry_after) + 0.35, 1.35), 90.0)
                print(
                    f"[TELEGRAM_429] retry_after={exc.retry_after}s attempt={attempt}/5",
                    flush=True,
                )
                if attempt < 5:
                    await asyncio.sleep(wait_for)
            except Exception as exc:
                last_error = exc
                telegram_diag["last_send_error"] = f"{type(exc).__name__}: {exc}"
                if attempt < 5:
                    await asyncio.sleep(min(1.5 * attempt, 6.0))

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

    desired_url = f"{PUBLIC_URL}/telegram/webhook"

    # Do not hammer setWebhook on every Render deploy. If Telegram already
    # points to this exact service, leave it untouched. This materially cuts
    # 429s during rapid redeploys / overlapping old+new instances.
    current_url = None
    try:
        info = await asyncio.to_thread(telegram_api, "getWebhookInfo", {})
        current_url = str((info.get("result") or {}).get("url") or "")
    except Exception as exc:
        print(f"[TELEGRAM_WEBHOOK_INFO_FAILED] {type(exc).__name__}: {exc}", flush=True)

    if current_url != desired_url:
        payload = {
            "url": desired_url,
            "drop_pending_updates": "false",
        }
        if TELEGRAM_WEBHOOK_SECRET:
            payload["secret_token"] = TELEGRAM_WEBHOOK_SECRET
        await asyncio.to_thread(telegram_api, "setWebhook", payload)
        telegram_diag["webhook_setup_action"] = "updated"
    else:
        telegram_diag["webhook_setup_action"] = "already_correct"
        print("[TELEGRAM_WEBHOOK] already correct; setWebhook skipped", flush=True)

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

    try:
        existing = await asyncio.to_thread(telegram_api, "getMyCommands", {})
        existing_commands = existing.get("result") or []
    except Exception:
        existing_commands = None

    if existing_commands != commands:
        try:
            await asyncio.to_thread(
                telegram_api,
                "setMyCommands",
                {"commands": json.dumps(commands)},
            )
            telegram_diag["commands_setup_action"] = "updated"
        except TelegramRateLimit as exc:
            # Commands are cosmetic. Do not make startup fail because Telegram
            # temporarily rate-limited setMyCommands.
            telegram_diag["commands_setup_action"] = f"rate_limited:{exc.retry_after}s"
            print(f"[TELEGRAM_COMMANDS_429] retry_after={exc.retry_after}s; skipped", flush=True)
    else:
        telegram_diag["commands_setup_action"] = "already_correct"




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


def _aggregate_complete_buckets_from_1m(one_minute_rows, minutes):
    """Build exact higher-timeframe OHLC from completed real 1m candles only.

    A bucket is accepted only when every constituent 1m candle exists at the
    expected 60-second timestamps. This is reconstruction, not synthetic data.
    """
    size = int(minutes) * 60
    by_ts = {}
    for row in one_minute_rows:
        if not isinstance(row, dict):
            continue
        try:
            ts = int(row.get("ts"))
            o = float(row.get("open"))
            h = float(row.get("high"))
            l = float(row.get("low"))
            c = float(row.get("close"))
        except (TypeError, ValueError):
            continue
        by_ts[ts] = {"ts": ts, "open": o, "high": h, "low": l, "close": c, "ticks": int(row.get("ticks") or 0)}

    grouped = {}
    for ts, row in by_ts.items():
        bucket = bucket_start(ts, minutes)
        grouped.setdefault(bucket, []).append(row)

    out = []
    expected_count = int(minutes)
    for bucket in sorted(grouped):
        rows = sorted(grouped[bucket], key=lambda x: x["ts"])
        expected = [bucket + 60 * i for i in range(expected_count)]
        actual = [r["ts"] for r in rows]
        if actual != expected:
            continue
        out.append({
            "ts": bucket,
            "open": rows[0]["open"],
            "high": max(r["high"] for r in rows),
            "low": min(r["low"] for r in rows),
            "close": rows[-1]["close"],
            "ticks": sum(int(r.get("ticks") or 0) for r in rows),
        })
    return out


def repair_index_timeframes_from_1m(symbol, force=False):
    """Repair 5m/15m histories from persisted real 1m candles.

    This fixes Render-restart warm-up without changing the V7.3 strategy.
    Existing higher-timeframe candles are preserved/merged by timestamp.
    """
    if symbol not in SIGNAL_SYMBOLS:
        return {"5m_added": 0, "15m_added": 0}

    one = list(candles_1m[symbol])
    result = {}
    for minutes, target, need in (
        (5, candles_5m[symbol], INDEX_NEED_5M),
        (15, candles_15m[symbol], INDEX_NEED_15M),
    ):
        if not force and len(target) >= need:
            result[f"{minutes}m_added"] = 0
            continue
        derived = _aggregate_complete_buckets_from_1m(one, minutes)
        merged = {
            int(c["ts"]): dict(c)
            for c in target
            if isinstance(c, dict) and c.get("ts") is not None
        }
        before = len(merged)
        for c in derived:
            merged[int(c["ts"])] = c
        ordered = [merged[k] for k in sorted(merged)]
        target.clear()
        for c in ordered[-target.maxlen:]:
            target.append(c)
        result[f"{minutes}m_added"] = max(0, len(merged) - before)
    return result


def repair_all_index_timeframes_from_1m(force=True):
    summary = {}
    for symbol in SIGNAL_SYMBOLS:
        summary[symbol] = repair_index_timeframes_from_1m(symbol, force=force)
    runtime["timeframe_repair"] = summary
    return summary


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


async def save_state(force=False):
    async with state_lock:
        try:
            if not force:
                previous = runtime.get("last_state_save_at")
                if previous:
                    try:
                        age = (datetime.now(timezone.utc) - datetime.fromisoformat(previous)).total_seconds()
                        if age < STATE_SAVE_MIN_INTERVAL_SECONDS:
                            runtime["last_state_save_skip"] = f"throttled:{int(age)}s"
                            return True
                    except Exception:
                        pass
            ok = await asyncio.to_thread(
                save_state_sync
            )

            if ok:
                runtime["last_state_save_at"] = (
                    datetime.now(
                        timezone.utc
                    ).isoformat()
                )
                runtime["last_state_save_error"] = None
                runtime["last_state_save_error_at"] = None

            return ok

        except Exception as exc:
            message = (
                f"State save: "
                f"{type(exc).__name__}: {exc}"
            )
            runtime["last_error"] = message

            # Avoid flooding Render logs with the exact same Gist error.
            now = datetime.now(timezone.utc)
            previous_error = runtime.get("last_state_save_error")
            previous_at = runtime.get("last_state_save_error_at")
            should_log = previous_error != message
            if not should_log and previous_at:
                try:
                    should_log = (
                        now - datetime.fromisoformat(previous_at)
                    ).total_seconds() >= 300
                except Exception:
                    should_log = True

            runtime["last_state_save_error"] = message
            runtime["last_state_save_error_at"] = now.isoformat()
            if should_log:
                print(
                    f"[STATE_SAVE_FAILED] {message}",
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

    # Critical restart repair: reconstruct missing 5m/15m bars from the
    # already-persisted REAL completed 1m candles. Strategy logic is untouched.
    repair_all_index_timeframes_from_1m(force=True)

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
            await save_state(force=True)

    print(
        f"[STATE_RESTORE] "
        f"loaded={loaded} "
        f"source={runtime.get('state_source')} "
        f"NIFTY=1m:{len(candles_1m['NIFTY 50'])}/5m:{len(candles_5m['NIFTY 50'])}/15m:{len(candles_15m['NIFTY 50'])} "
        f"SENSEX=1m:{len(candles_1m['SENSEX'])}/5m:{len(candles_5m['SENSEX'])}/15m:{len(candles_15m['SENSEX'])}",
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

        "risk_per_unit":
            tick(risk),

        "rr_target_1":
            "1:1",

        "rr_target_2":
            "1:2",

        "basis":
            "selected option LTP; 15% signal-only risk model",
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

    runtime["evaluations"] += 1
    runtime["last_evaluation_at"] = datetime.now(timezone.utc).isoformat()

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

    runtime["last_score_by_symbol"][symbol] = {
        "direction": direction,
        "score": score,
        "grade": grade,
        "at": datetime.now(timezone.utc).isoformat(),
    }

    if blockers:
        runtime["warming_up"] += 1
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
        runtime["no_trade"] += 1
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
    runtime["technical_actionable"] += 1
    try:
        discovery = await asyncio.wait_for(
            auto_discover_option(
                symbol,
                direction,
            ),
            timeout=30,
        )

    except Exception as exc:
        runtime["option_errors"] += 1
        runtime["last_index_blocker"] = (
            f"{symbol}: option discovery error: "
            f"{type(exc).__name__}: {exc}"
        )
        return

    if not discovery.get("ready"):
        runtime["option_errors"] += 1
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
        runtime["option_filtered"] += 1
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
        runtime["final_actionable"] += 1
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
    closed_1m = False
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

            if minutes == 1:
                closed_1m = True
            if minutes == 5:
                closed_5m = True

    # After a restart, rebuild any missing complete 5m/15m bars from real 1m
    # history before the unchanged V7.3 strategy evaluates.
    if closed_1m:
        repair_index_timeframes_from_1m(symbol, force=False)

    # Evaluate on every completed candle boundary.
    # With previous state restored, this can work from 09:30 onward.
    if closed_any:
        await evaluate_index_signal(
            symbol
        )

    # Persist on 5-minute boundaries to keep GitHub writes modest.
    if closed_5m:
        # A completed 5m candle is valuable warm-up state. Force-persist it so
        # a Render restart cannot throw away the session's higher-TF history.
        await save_state(force=True)


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
        # A completed 5m candle is valuable warm-up state. Force-persist it so
        # a Render restart cannot throw away the session's higher-TF history.
        await save_state(force=True)


# =========================================================
# BEST-EFFORT OFFICIAL HISTORICAL BACKFILL
# =========================================================
INDEX_HISTORY_SPEC = {
    "NIFTY 50": {
        "exchange_segment": "nse_cm",
        "instrument_token": "Nifty 50",
        "neosymbol": "nse_cm|Nifty 50",
    },
    "SENSEX": {
        "exchange_segment": "bse_cm",
        "instrument_token": "SENSEX",
        "neosymbol": "bse_cm|SENSEX",
    },
}


def index_history_ready(symbol):
    return (
        len(candles_1m[symbol]) >= INDEX_NEED_1M
        and len(candles_5m[symbol]) >= INDEX_NEED_5M
        and len(candles_15m[symbol]) >= INDEX_NEED_15M
    )


def all_index_history_ready():
    return all(index_history_ready(symbol) for symbol in SIGNAL_SYMBOLS)


def _historical_rows(response):
    """Normalize official SDK historical responses without fabricating data."""
    if response is None:
        return []

    # pandas DataFrame (some SDK/service layers return tabular data)
    if hasattr(response, "to_dict"):
        try:
            records = response.to_dict("records")
            if isinstance(records, list):
                return records
        except Exception:
            pass

    if isinstance(response, list):
        return response

    if not isinstance(response, dict):
        return []

    # Search common envelopes recursively, but only return actual row lists.
    queue = [response]
    seen = set()
    while queue:
        node = queue.pop(0)
        if id(node) in seen:
            continue
        seen.add(id(node))
        if isinstance(node, list):
            return node
        if not isinstance(node, dict):
            continue
        for key in ("candles", "records", "items", "values", "result", "results", "data"):
            value = node.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                queue.append(value)
    return []


def _historical_timestamp(value):
    """Parse SDK candle timestamps safely (epoch sec/ms or common date strings)."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw > 10_000_000_000:
            raw /= 1000.0
        return int(raw)
    text = str(value).strip()
    if not text:
        return None
    try:
        raw = float(text)
        if raw > 10_000_000_000:
            raw /= 1000.0
        return int(raw)
    except Exception:
        pass
    text = text.replace("Z", "+00:00")
    for fmt in (None, "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d-%b-%Y %H:%M:%S"):
        try:
            dt = datetime.fromisoformat(text) if fmt is None else datetime.strptime(text, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=IST)
            return int(dt.timestamp())
        except Exception:
            continue
    return None


def _historical_candle(row, minutes):
    """Convert dict/list historical rows into KING BRO candle format."""
    ts = opn = high = low = close = None

    if isinstance(row, dict):
        def pick(*names):
            for name in names:
                if row.get(name) not in (None, ""):
                    return row.get(name)
            return None

        ts = _historical_timestamp(
            pick(
                "ts", "timestamp", "time", "datetime", "date",
                "exchange_time", "exchange_timestamp", "start_time",
            )
        )
        opn = number(pick("open", "Open", "o"))
        high = number(pick("high", "High", "h"))
        low = number(pick("low", "Low", "l"))
        close = number(pick("close", "Close", "c", "ltp"))

    elif isinstance(row, (list, tuple)) and len(row) >= 5:
        # Most candle APIs use [timestamp, open, high, low, close, ...].
        ts = _historical_timestamp(row[0])
        opn = number(row[1])
        high = number(row[2])
        low = number(row[3])
        close = number(row[4])

    if None in (ts, opn, high, low, close):
        return None

    if high < low:
        return None

    bucket = bucket_start(ts, minutes)

    # Do not import the current still-forming candle.
    current_bucket = bucket_start(int(datetime.now(timezone.utc).timestamp()), minutes)
    if bucket >= current_bucket:
        return None

    return {
        "ts": int(bucket),
        "open": float(opn),
        "high": float(high),
        "low": float(low),
        "close": float(close),
        "ticks": 0,
        "source": "kotak_historical",
    }


def _merge_history(target, rows, minutes):
    merged = {int(c.get("ts")): dict(c) for c in target if isinstance(c, dict) and c.get("ts") is not None}
    before = len(merged)

    for row in rows:
        candle = _historical_candle(row, minutes)
        if candle is not None:
            merged[int(candle["ts"])] = candle

    ordered = [merged[key] for key in sorted(merged)]
    target.clear()
    for candle in ordered[-target.maxlen:]:
        target.append(candle)

    return max(0, len(merged) - before)


def _historical_call_sync(client, exchange_segment, instrument_token, interval, neosymbol=None):
    """
    Kotak Neo v3.0.6 adapter.

    The running SDK has reported:
      historical_data(neosymbol, interval, from_date, to_date)

    Use that exact signature when present. Older/alternate signatures remain
    supported by introspection, but we never send exchange_segment or
    instrument_token to a method that only accepts neosymbol.
    """
    method = getattr(client, "historical_data", None)
    if not callable(method):
        raise RuntimeError("Installed Kotak SDK has no historical_data()")

    end_dt = datetime.now(IST)
    start_dt = end_dt - timedelta(days=max(2, HISTORICAL_LOOKBACK_DAYS))

    # SDK docs/runtime signature require strings. Try the normal ISO date first;
    # API-format errors are retried with common timestamp forms below.
    date_pairs = [
        (start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")),
        (start_dt.strftime("%d-%m-%Y"), end_dt.strftime("%d-%m-%Y")),
        (start_dt.strftime("%Y-%m-%d %H:%M:%S"), end_dt.strftime("%Y-%m-%d %H:%M:%S")),
    ]

    neo = neosymbol or f"{exchange_segment}|{instrument_token}"

    try:
        signature = inspect.signature(method)
        params = signature.parameters
        runtime["historical_sdk_signature"] = str(signature)
    except Exception as exc:
        params = {}
        runtime["historical_sdk_signature"] = f"unavailable:{type(exc).__name__}:{exc}"

    names = {name for name in params if name != "self"}

    # Exact v3.0.6 path observed on the deployed service.
    if {"neosymbol", "interval", "from_date", "to_date"}.issubset(names):
        last_exc = None
        for from_date, to_date in date_pairs:
            try:
                runtime["historical_last_request"] = {
                    "neosymbol": neo,
                    "interval": interval,
                    "from_date": from_date,
                    "to_date": to_date,
                }
                return method(
                    neosymbol=neo,
                    interval=interval,
                    from_date=from_date,
                    to_date=to_date,
                )
            except TypeError:
                raise
            except Exception as exc:
                last_exc = exc
                # Retry only date formatting; caller will try interval aliases.
                continue
        if last_exc:
            raise last_exc

    # Compatibility path for any future/alternate SDK shape.
    start_date, end_date = date_pairs[0]
    start_iso, end_iso = date_pairs[2]
    aliases = {
        "neosymbol": neo,
        "exchange_segment": exchange_segment,
        "exchange": exchange_segment,
        "segment": exchange_segment,
        "instrument_token": instrument_token,
        "token": instrument_token,
        "symbol": instrument_token,
        "instrument": instrument_token,
        "interval": interval,
        "timeframe": interval,
        "time_frame": interval,
        "resolution": interval,
        "from_date": start_date,
        "fromdate": start_date,
        "start_date": start_date,
        "to_date": end_date,
        "todate": end_date,
        "end_date": end_date,
        "start_time": start_iso,
        "from_time": start_iso,
        "end_time": end_iso,
        "to_time": end_iso,
    }

    kwargs = {}
    missing = []
    for name, param in params.items():
        if name == "self":
            continue
        if name in aliases:
            kwargs[name] = aliases[name]
        elif param.default is inspect._empty and param.kind not in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            missing.append(name)

    if params and not missing:
        runtime["historical_last_request"] = dict(kwargs)
        return method(**kwargs)

    raise RuntimeError(
        f"Unsupported historical_data signature: {runtime.get('historical_sdk_signature')}"
    )


def historical_backfill_sync(client):
    """
    Fill the ORIGINAL V7.3 warm-up from Kotak historical candles after login.

    Primary path:
      1) request completed 1-minute index candles
      2) merge them with Gist/live 1m history
      3) reconstruct exact complete 5m and 15m OHLC buckets from those real 1m bars

    Direct 5m/15m historical requests are only a fallback. No synthetic candles
    are invented and the original 22/21/9 strategy gate is unchanged.
    """
    total = 0
    errors = []

    interval_variants = {
        1: ("1minute", "1min", "1m", "minute", "1"),
        5: ("5minute", "5min", "5m", "5"),
        15: ("15minute", "15min", "15m", "15"),
    }

    for symbol in SIGNAL_SYMBOLS:
        spec = INDEX_HISTORY_SPEC[symbol]

        # First get enough REAL 1m history. This is the most reliable way to
        # make all three timeframes internally consistent.
        one_ok = len(candles_1m[symbol]) >= 150
        last_error = None
        if not one_ok:
            for interval in interval_variants[1]:
                try:
                    response = _historical_call_sync(
                        client,
                        spec["exchange_segment"],
                        spec["instrument_token"],
                        interval,
                        spec.get("neosymbol"),
                    )
                    rows = _historical_rows(response)
                    if rows:
                        added = _merge_history(candles_1m[symbol], rows, 1)
                        total += added
                        if len(candles_1m[symbol]) >= INDEX_NEED_1M:
                            break
                    else:
                        last_error = f"{interval}: no candle rows returned"
                except Exception as exc:
                    last_error = f"{interval}: {type(exc).__name__}: {exc}"

        # Exact aggregation from completed real 1m candles.
        repair_index_timeframes_from_1m(symbol, force=True)

        # If 1m historical endpoint did not provide a long enough contiguous
        # run, ask Kotak directly for the missing higher timeframe.
        for minutes, target, need in (
            (5, candles_5m[symbol], INDEX_NEED_5M),
            (15, candles_15m[symbol], INDEX_NEED_15M),
        ):
            if len(target) >= need:
                continue
            tf_error = None
            for interval in interval_variants[minutes]:
                try:
                    response = _historical_call_sync(
                        client,
                        spec["exchange_segment"],
                        spec["instrument_token"],
                        interval,
                        spec.get("neosymbol"),
                    )
                    rows = _historical_rows(response)
                    if rows:
                        total += _merge_history(target, rows, minutes)
                        if len(target) >= need:
                            break
                    else:
                        tf_error = f"{interval}: no candle rows returned"
                except Exception as exc:
                    tf_error = f"{interval}: {type(exc).__name__}: {exc}"
            if len(target) < need and tf_error:
                errors.append(f"{symbol} {minutes}m: {tf_error}")

        if len(candles_1m[symbol]) < INDEX_NEED_1M and last_error:
            errors.append(f"{symbol} 1m: {last_error}")

    runtime["historical_backfill_attempted"] = True
    runtime["historical_backfill_loaded"] = total
    runtime["historical_backfill_error"] = "; ".join(errors[:6]) if errors else None

    print(
        "[HISTORY_READY_CHECK] "
        f"NIFTY=1m:{len(candles_1m['NIFTY 50'])}/5m:{len(candles_5m['NIFTY 50'])}/15m:{len(candles_15m['NIFTY 50'])} "
        f"SENSEX=1m:{len(candles_1m['SENSEX'])}/5m:{len(candles_5m['SENSEX'])}/15m:{len(candles_15m['SENSEX'])} "
        f"loaded={total} error={runtime.get('historical_backfill_error')}",
        flush=True,
    )
    return total


async def historical_backfill_if_needed(client):
    if not HISTORICAL_BACKFILL_ENABLED:
        return 0

    # Gist history is the primary path. Historical REST is only a gap filler.
    needs_backfill = not all_index_history_ready()
    if not needs_backfill:
        needs_backfill = any(not refresh_daily_levels(symbol).get("ready") for symbol in SIGNAL_SYMBOLS)

    if not needs_backfill:
        runtime["historical_backfill_attempted"] = False
        runtime["historical_backfill_error"] = None
        return 0

    try:
        loaded = await asyncio.to_thread(historical_backfill_sync, client)
        if loaded:
            repair_all_index_timeframes_from_1m(force=True)
            runtime["state_source"] = "gist_plus_kotak_historical"
            await save_state(force=True)
            print(f"[HISTORICAL_BACKFILL] loaded={loaded}", flush=True)
        elif runtime.get("historical_backfill_error"):
            print(
                f"[HISTORICAL_BACKFILL_SKIPPED] {runtime['historical_backfill_error']}",
                flush=True,
            )
        return loaded
    except Exception as exc:
        runtime["historical_backfill_attempted"] = True
        runtime["historical_backfill_error"] = f"{type(exc).__name__}: {exc}"
        print(f"[HISTORICAL_BACKFILL_FAILED] {runtime['historical_backfill_error']}", flush=True)
        return 0


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
    runtime["relogin_required"] = False
    runtime["last_relogin_notice_at"] = None

    # Fill only missing/stale candle history. Gist remains primary; the
    # official Kotak historical endpoint is a best-effort gap filler.
    await historical_backfill_if_needed(client)

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


def looks_like_auth_error(exc):
    text = str(exc or "").lower()
    return any(
        token in text
        for token in (
            "401", "403", "unauthor", "forbidden",
            "invalid session", "session expired", "login required",
            "invalid token", "auth failed",
        )
    )


async def send_relogin_notice(reason):
    now = datetime.now(timezone.utc)
    previous = runtime.get("last_relogin_notice_at")
    if previous:
        try:
            age = (now - datetime.fromisoformat(previous)).total_seconds()
            if age < 300:
                return
        except Exception:
            pass

    ok = await telegram_send(
        "⚠️ KING BRO — KOTAK RELOGIN REQUIRED\n\n"
        f"Reason: {reason}\n"
        "Candle history is safe in Gist.\n"
        "Send /login CURRENT_TOTP once to resume live signals.",
        with_keyboard=True,
    )
    if ok:
        runtime["last_relogin_notice_at"] = now.isoformat()


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

            if looks_like_auth_error(exc):
                runtime["broker_connected"] = False
                runtime["relogin_required"] = True
                neo_client = None
                await send_relogin_notice(runtime["last_error"])
                return

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

            if looks_like_auth_error(exc):
                runtime["broker_connected"] = False
                runtime["relogin_required"] = True
                neo_client = None
                await send_relogin_notice(runtime["stock_error"])
                return

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
    ready = index_history_ready(symbol)
    return (
        f"{'✅' if ready else '⚠️'} {symbol}: "
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
        tick_age = last_tick_age_seconds()
        scores = runtime.get("last_score_by_symbol") or {}
        await telegram_send(
            f"👑 KING BRO {APP_VERSION} STATUS\n\n"
            f"Broker: "
            f"{'CONNECTED' if runtime['broker_connected'] else 'OFFLINE'}\n"
            f"Index feed: "
            f"{'LIVE' if runtime['index_feed_connected'] else 'OFFLINE'}\n"
            f"Re-login required: "
            f"{'YES ⚠️' if runtime.get('relogin_required') else 'NO'}\n"
            f"Last tick age: "
            f"{int(tick_age) if tick_age is not None else 'N/A'} sec\n"
            f"Index signals: "
            f"{'ON' if runtime['index_scan_enabled'] else 'OFF'}\n"
            f"Signal window: "
            f"{'ACTIVE' if signal_window_open() else 'INACTIVE'}\n\n"
            f"{index_readiness_text('NIFTY 50')}\n"
            f"{index_readiness_text('SENSEX')}\n"
            f"State source: {runtime.get('state_source')}\n"
            f"Last save: {runtime.get('last_state_save_at')}\n"
            f"Gist error: {runtime.get('last_state_save_error')}\n"
            f"Historical preload attempted: "
            f"{'YES' if runtime.get('historical_backfill_attempted') else 'NO'}\n"
            f"Historical loaded: {runtime.get('historical_backfill_loaded', 0)}\n"
            f"Historical error: {runtime.get('historical_backfill_error') or 'None'}\n"
            f"Historical SDK: {runtime.get('historical_sdk_signature') or 'not inspected'}\n"
            f"Historical request: {runtime.get('historical_last_request') or 'None'}\n"
            f"TF repair: {runtime.get('timeframe_repair') or {}}\n\n"
            f"Evaluations: {runtime.get('evaluations', 0)}\n"
            f"Technical actionable: {runtime.get('technical_actionable', 0)}\n"
            f"Final actionable: {runtime.get('final_actionable', 0)}\n"
            f"Option filtered/errors: "
            f"{runtime.get('option_filtered', 0)}/{runtime.get('option_errors', 0)}\n"
            f"Warming/No-trade: "
            f"{runtime.get('warming_up', 0)}/{runtime.get('no_trade', 0)}\n"
            f"Last scores: {scores}\n"
            f"Last blocker: {runtime.get('last_index_blocker')}\n\n"
            f"Supervisor: "
            f"{'ON' if runtime.get('supervisor_running') else 'OFF'}\n"
            f"Feed restarts: {runtime.get('feed_restart_count', 0)}\n"
            f"Last feed restart: {runtime.get('last_feed_restart_at')}\n"
            f"Keepalive: {runtime.get('last_keepalive_ok')}\n\n"
            f"Stock scan/feed: "
            f"{'ON' if runtime['stock_scan_enabled'] else 'OFF'}/"
            f"{'LIVE' if runtime['stock_feed_connected'] else 'OFFLINE'}\n"
            f"Stocks resolved: {runtime['stock_resolved']}/{len(STOCK_UNIVERSE)}\n"
            f"Last error: {runtime.get('last_error')}",
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
        ok = await save_state(force=True)

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
@app.get("/api/telegram/diag")
async def telegram_diagnostics():
    result = {
        "configured": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
        "public_webhook_target": f"{PUBLIC_URL}/telegram/webhook" if PUBLIC_URL else None,
        "send": dict(telegram_diag),
    }

    if TELEGRAM_BOT_TOKEN:
        try:
            info = await asyncio.to_thread(telegram_api, "getWebhookInfo", {})
            data = info.get("result") or {}
            result["webhook"] = {
                "url": data.get("url"),
                "pending_update_count": data.get("pending_update_count"),
                "last_error_date": data.get("last_error_date"),
                "last_error_message": data.get("last_error_message"),
                "max_connections": data.get("max_connections"),
            }
        except Exception as exc:
            result["webhook_error"] = f"{type(exc).__name__}: {exc}"

    return result


@app.get("/")
async def root():
    return {
        "service":
            "KING BRO V7 Final All Fixed",

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
    tick_age = last_tick_age_seconds()
    return {
        "ok": True,
        "version": APP_VERSION,
        "time_ist": ist_now().isoformat(),
        "broker_connected": runtime["broker_connected"],
        "relogin_required": runtime.get("relogin_required"),
        "index_feed_connected": runtime["index_feed_connected"],
        "last_tick_at": runtime.get("last_tick_at"),
        "last_tick_age_seconds": tick_age,
        "index_scan_enabled": runtime["index_scan_enabled"],
        "feed_window_active": feed_window_open(),
        "signal_window_active": signal_window_open(),
        "history_ready": all_index_history_ready(),
        "nifty": {
            "1m": len(candles_1m["NIFTY 50"]),
            "5m": len(candles_5m["NIFTY 50"]),
            "15m": len(candles_15m["NIFTY 50"]),
            "ready": index_history_ready("NIFTY 50"),
        },
        "sensex": {
            "1m": len(candles_1m["SENSEX"]),
            "5m": len(candles_5m["SENSEX"]),
            "15m": len(candles_15m["SENSEX"]),
            "ready": index_history_ready("SENSEX"),
        },
        "diagnostics": {
            "evaluations": runtime.get("evaluations", 0),
            "warming_up": runtime.get("warming_up", 0),
            "no_trade": runtime.get("no_trade", 0),
            "technical_actionable": runtime.get("technical_actionable", 0),
            "option_filtered": runtime.get("option_filtered", 0),
            "option_errors": runtime.get("option_errors", 0),
            "final_actionable": runtime.get("final_actionable", 0),
            "last_evaluation_at": runtime.get("last_evaluation_at"),
            "last_score_by_symbol": runtime.get("last_score_by_symbol"),
            "last_index_blocker": runtime.get("last_index_blocker"),
        },
        "reliability": {
            "supervisor_running": runtime.get("supervisor_running"),
            "feed_restart_count": runtime.get("feed_restart_count", 0),
            "last_feed_restart_at": runtime.get("last_feed_restart_at"),
            "last_keepalive_at": runtime.get("last_keepalive_at"),
            "last_keepalive_ok": runtime.get("last_keepalive_ok"),
            "keepalive_market_only": True,
        },
        "persistence": {
            "configured": bool(GITHUB_TOKEN and STATE_GIST_ID),
            "state_source": runtime.get("state_source"),
            "last_state_save_at": runtime.get("last_state_save_at"),
            "last_state_save_error": runtime.get("last_state_save_error"),
            "historical_backfill_attempted": runtime.get("historical_backfill_attempted"),
            "historical_backfill_loaded": runtime.get("historical_backfill_loaded"),
            "historical_backfill_error": runtime.get("historical_backfill_error"),
        },
        "stocks": {
            "scan_enabled": runtime["stock_scan_enabled"],
            "feed_connected": runtime["stock_feed_connected"],
            "resolved": runtime.get("stock_resolved", 0),
            "universe": len(STOCK_UNIVERSE),
        },
        "execution": "MANUAL_ONLY",
        "last_error": runtime.get("last_error"),
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
# RELIABILITY: KEEPALIVE + FEED SUPERVISOR + PERIODIC SAVE
# =========================================================
keepalive_task: Optional[asyncio.Task] = None
supervisor_task: Optional[asyncio.Task] = None
autosave_task: Optional[asyncio.Task] = None


def _keepalive_ping_sync(url: str) -> bool:
    """Best-effort self-ping. Never raises to the event loop."""
    try:
        request = urllib.request.Request(
            url,
            method="GET",
            headers={
                "User-Agent": "kingbro-v7-market-keepalive",
                "Accept": "application/json",
            },
        )
        with urllib.request.urlopen(request, timeout=15) as response:
            return 200 <= response.status < 300
    except Exception as exc:
        print(
            f"[KEEPALIVE_FAIL] {type(exc).__name__}: {exc}",
            flush=True,
        )
        return False


def last_tick_age_seconds():
    value = runtime.get("last_tick_at")
    if not value:
        return None
    try:
        return max(
            0.0,
            (
                datetime.now(timezone.utc)
                - datetime.fromisoformat(value)
            ).total_seconds(),
        )
    except Exception:
        return None


async def keepalive_loop():
    """
    Self-ping only in the 09:00–15:40 IST feed window.
    The external GitHub Actions ping in .github/workflows is the primary
    anti-idle guard because an in-process loop cannot wake a stopped process.
    """
    if not KEEPALIVE_URL:
        print(
            "[KEEPALIVE] disabled — no PUBLIC_URL / KINGBRO_KEEPALIVE_URL",
            flush=True,
        )
        return

    print(
        f"[KEEPALIVE] market-only interval={KEEPALIVE_INTERVAL_SECONDS}s "
        f"url={KEEPALIVE_URL}",
        flush=True,
    )

    await asyncio.sleep(15)

    while True:
        try:
            if feed_window_open():
                ok = await asyncio.to_thread(
                    _keepalive_ping_sync,
                    KEEPALIVE_URL,
                )
                runtime["last_keepalive_at"] = (
                    datetime.now(timezone.utc).isoformat()
                )
                runtime["last_keepalive_ok"] = ok
                await asyncio.sleep(max(120, KEEPALIVE_INTERVAL_SECONDS))
            else:
                # Do not burn Render Free hours overnight/weekends.
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            runtime["last_keepalive_ok"] = False
            print(
                f"[KEEPALIVE_ERROR] {type(exc).__name__}: {exc}",
                flush=True,
            )
            await asyncio.sleep(60)


async def restart_index_feed(reason: str):
    global index_feed_task

    if neo_client is None or not runtime.get("broker_connected"):
        return False

    if index_feed_task and not index_feed_task.done():
        index_feed_task.cancel()
        try:
            await index_feed_task
        except (asyncio.CancelledError, Exception):
            pass

    runtime["index_feed_connected"] = False
    runtime["feed_restart_count"] = int(runtime.get("feed_restart_count") or 0) + 1
    runtime["last_feed_restart_at"] = datetime.now(timezone.utc).isoformat()
    print(f"[SUPERVISOR_RESTART_INDEX] {reason}", flush=True)

    index_feed_task = asyncio.create_task(
        index_feed_loop(),
        name="kingbro-index-feed",
    )
    return True


async def restart_stock_feed(reason: str):
    global stock_feed_task

    if (
        not runtime.get("stock_scan_enabled")
        or neo_client is None
        or not runtime.get("broker_connected")
    ):
        return False

    if stock_feed_task and not stock_feed_task.done():
        stock_feed_task.cancel()
        try:
            await stock_feed_task
        except (asyncio.CancelledError, Exception):
            pass

    runtime["stock_feed_connected"] = False
    print(f"[SUPERVISOR_RESTART_STOCK] {reason}", flush=True)
    stock_feed_task = asyncio.create_task(
        stock_feed_loop(),
        name="kingbro-stock-feed",
    )
    return True


async def supervisor_loop():
    """
    Keeps an authenticated session's WebSocket feeds alive.
    It can reconnect WebSockets without a new TOTP. It cannot recreate a
    Kotak authenticated client after the whole Render process is destroyed;
    in that case it sends a Telegram re-login alert instead of pretending the
    session was restored.
    """
    runtime["supervisor_running"] = True
    print(
        f"[SUPERVISOR] started stale_threshold={FEED_STALE_SECONDS}s",
        flush=True,
    )

    try:
        while True:
            await asyncio.sleep(30)

            if not feed_window_open():
                continue

            if neo_client is None or not runtime.get("broker_connected"):
                runtime["relogin_required"] = True
                await send_relogin_notice(
                    "Render/Kotak session is not authenticated during market hours."
                )
                continue

            # Feed coroutine died unexpectedly: recreate it with the same
            # authenticated NeoAPI object — no new TOTP required.
            if index_feed_task is None or index_feed_task.done():
                await restart_index_feed("index feed task stopped")
                continue

            # A connected feed that stopped delivering index ticks is stale.
            tick_age = last_tick_age_seconds()
            if (
                runtime.get("index_feed_connected")
                and tick_age is not None
                and tick_age > FEED_STALE_SECONDS
            ):
                await restart_index_feed(
                    f"last index tick is {int(tick_age)}s old"
                )

            if runtime.get("stock_scan_enabled"):
                if stock_feed_task is None or stock_feed_task.done():
                    await restart_stock_feed("stock feed task stopped")
    except asyncio.CancelledError:
        return
    finally:
        runtime["supervisor_running"] = False


async def autosave_loop():
    """Extra safety net; normal 5-minute candle-close saves remain unchanged."""
    await asyncio.sleep(45)
    while True:
        try:
            if feed_window_open() and (GITHUB_TOKEN and STATE_GIST_ID):
                await save_state()
                await asyncio.sleep(max(120, AUTO_STATE_SAVE_SECONDS))
            else:
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            print(
                f"[AUTOSAVE_ERROR] {type(exc).__name__}: {exc}",
                flush=True,
            )
            await asyncio.sleep(60)


# =========================================================
# STARTUP / SHUTDOWN
# =========================================================
@app.on_event("startup")
async def startup():
    global keepalive_task, supervisor_task, autosave_task

    loaded = await restore_state()

    try:
        await setup_telegram()
    except Exception as exc:
        print(
            f"[TELEGRAM_SETUP_FAILED] {type(exc).__name__}: {exc}",
            flush=True,
        )

    if KEEPALIVE_ENABLED and KEEPALIVE_URL:
        keepalive_task = asyncio.create_task(
            keepalive_loop(),
            name="kingbro-market-keepalive",
        )

    if SUPERVISOR_ENABLED:
        supervisor_task = asyncio.create_task(
            supervisor_loop(),
            name="kingbro-feed-supervisor",
        )

    autosave_task = asyncio.create_task(
        autosave_loop(),
        name="kingbro-gist-autosave",
    )

    # Keep Render startup Telegram-quiet. During a rolling deploy the old and
    # new instances overlap briefly; automatic startup messages from both can
    # trigger Telegram 429 flood control and then block /status replies.
    # User commands remain active immediately. Only a genuine market-hours
    # relogin warning is scheduled, and telegram_send() itself obeys 429
    # retry_after + per-chat pacing.
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID and feed_window_open() and neo_client is None:
        runtime["relogin_required"] = True

        async def _delayed_market_relogin_notice():
            await asyncio.sleep(6)
            await send_relogin_notice(
                "Backend started/restarted during the market feed window."
            )

        asyncio.create_task(
            _delayed_market_relogin_notice(),
            name="kingbro-startup-relogin-notice",
        )


@app.on_event("shutdown")
async def shutdown():
    global keepalive_task, supervisor_task, autosave_task
    global index_feed_task, stock_feed_task

    # Best effort only: a hard platform kill may not give shutdown enough time.
    if GITHUB_TOKEN and STATE_GIST_ID:
        try:
            await asyncio.wait_for(save_state(force=True), timeout=8)
        except Exception as exc:
            print(
                f"[SHUTDOWN_SAVE_FAILED] {type(exc).__name__}: {exc}",
                flush=True,
            )

    for task in (
        index_feed_task,
        stock_feed_task,
        keepalive_task,
        supervisor_task,
        autosave_task,
    ):
        if task and not task.done():
            task.cancel()
