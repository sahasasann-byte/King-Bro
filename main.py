import asyncio
import re
from datetime import datetime, timezone, date
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
    version="5.4.0",
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


# Latest inspected option intelligence, keyed by "exchange|token".
option_intelligence: dict[str, dict[str, Any]] = {}

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
        "mock_data": False,
        "received_at":
            datetime.now(timezone.utc).isoformat(),
    }


def _quotes_sync(
    instrument_token: str,
    exchange_segment: str,
):
    client = NeoAPI(
        consumer_key=settings.KOTAK_CONSUMER_KEY,
        environment=settings.KOTAK_ENVIRONMENT,
    )

    response = client.quotes(
        instrument_tokens=[
            {
                "instrument_token":
                    str(instrument_token),
                "exchange_segment":
                    str(exchange_segment),
            }
        ],
        quote_type="all",
    )

    rows = _normalise_rows(response)

    if not rows:
        if isinstance(response, dict):
            message = (
                response.get("message")
                or response.get("emsg")
                or response.get("error")
                or response.get("status")
            )
            raise RuntimeError(
                "Kotak Quotes returned no instrument data"
                + (f": {message}" if message else "")
                + f". Raw response keys={list(response.keys())}"
            )

        raise RuntimeError(
            f"Kotak Quotes returned no instrument data. "
            f"Raw response type={type(response).__name__}"
        )

    return rows[0]


def _search_option_sync(
    exchange_segment: str,
    symbol: str,
    expiry: str = "",
    option_type: str = "",
    strike_price: str = "",
):
    client = NeoAPI(
        consumer_key=settings.KOTAK_CONSUMER_KEY,
        environment=settings.KOTAK_ENVIRONMENT,
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

    match = re.search(r"(\\d+(?:\\.\\d+)?)(CE|PE)$", ts)

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
                "but Kotak Quotes returned no usable live option LTP."
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
        "selector_version": "5.3",
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
