# KING BRO V7.7 — CLASSIC BREAKOUT ENGINE RESTORED

## Why this release exists
The recent V7.x signal path could identify a strong technical setup and then suppress the Telegram alert when option LTP/OI/liquidity enrichment was incomplete. The user specifically requested the earlier breakout engine that was better suited to sharp expiry-day moves.

## Primary index signal engine
NIFTY 50 and SENSEX now use the restored classic 5-minute trend-aligned breakout engine as the PRIMARY signal decision:

- completed 5-minute candles only
- 20-candle prior support/resistance lookback
- fast mean = last 5 closes
- slow mean = prior 20 closes
- CALL only when the latest completed 5M close breaks the prior resistance AND fast > slow
- PUT only when the latest completed 5M close breaks the prior support AND fast < slow
- ATR-like recent range risk model
- minimum R:R = 1:1.85
- T2 = 2.30R
- classic score = 60 + trend strength + breakout strength, capped at 95
- no separate min-score veto: a confirmed classic breakout is the signal

Grades are display labels only:
- 80+ = A+
- 70-79 = STRONG
- 60-69 = BREAKOUT

All three confirmed-breakout grades are Telegram eligible, subject to the existing 15-minute duplicate cooldown.

## Option data is enrichment, not a signal killer
Nearest-expiry ATM option discovery is retained. Kotak LTP, OI, depth and liquidity are still fetched when available. However:

- OPTION_NOT_READY no longer cancels a confirmed classic breakout
- weak/incomplete option quality no longer cancels the technical alert
- option problems are shown as warnings / option_status
- if Kotak search identifies a nearest-expiry contract but LTP is temporarily unavailable, the contract identity is retained when possible

When a usable option LTP is available, the option signal plan uses:
- 15% stop model
- T1 = 1.85R
- T2 = 2.30R

No auto order is added by this change.

## What stays from V7.6
- Render market-hours keepalive
- feed stale watchdog + reconnect
- restart/TOTP diagnostics
- Kotak quote fallbacks: all -> ltp / oi / market depth as required
- compact dashboard and live market sentiment
- Stock Scan START/STOP control
- Telegram retry/cooldown diagnostics

## Log to look for
The index engine now prints:

`[CLASSIC_SIGNAL_EVAL] SENSEX direction=PUT score=... grade=... actionable=... classic=SIGNAL ...`

A normal no-trade evaluation prints `classic=REJECTED reason=NO_CONFIRMED_BREAKOUT`.

## Validation performed
- Python compile: PASS
- synthetic CALL breakout: PASS
- synthetic PUT breakout: PASS
- no-breakout rejection: PASS
- minimum R:R 1.85: PASS
- option plan T1 1.85R / T2 2.30R: PASS
