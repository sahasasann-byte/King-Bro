# KING BRO V7 FINAL — Telegram Only / Manual Signals

This build is the reliability-fixed successor to V6.1 and the Grok keepalive patch.
It keeps the user's **original V7.3 multi-timeframe scoring strategy** as the primary
NIFTY/SENSEX signal engine. It does not place orders.

## What V7 fixes

- **Original V7.3 engine preserved**: 22×1m + 21×5m + 9×15m warm-up; A+ 80+, STRONG 70+.
- **Durable Gist state**: index/stock candle history is saved automatically and restored after restart.
- **Larger 1m retention (900)**: enough to keep previous-session candles for daily CPR/Fib context instead of dropping them near the end of the next session.
- **Automatic Gist save**: normal 5m-close save plus a 5-minute safety autosave loop.
- **Historical gap fill**: after successful TOTP login, kotakneoapi 3.0.6 `historical_data()` is tried only when Gist history is insufficient. This is best-effort; failure never fabricates data or changes the strategy.
- **Feed supervisor**: if an authenticated WebSocket task dies or index ticks go stale, the feed is recreated with the same authenticated NeoAPI client, so ordinary feed reconnects do not need another TOTP.
- **Auth-expiry detection**: 401/403/session-expired style failures stop reconnect loops and send a clear Telegram **RELOGIN REQUIRED** warning.
- **Market-hours keepalive only**: internal self-ping operates only 09:00–15:40 IST instead of wasting free instance hours all night.
- **External GitHub Actions keepalive**: pings `/health` every 5 minutes during the market feed window. This can wake/keep the service reachable even when an in-process self-ping cannot.
- **Better diagnostics**: `/status` and `/health` expose evaluations, technical signals, option filters/errors, last scores, feed restarts, last tick age, state save status, and historical backfill status.
- **Stock scanner**: original 40-stock universe preserved, default OFF.
- **No automatic execution**: no `place_order`, `modify_order`, `cancel_order`, or square-off code.

## Daily operation

1. Around 09:00–09:20 IST send `/login CURRENT_6_DIGIT_TOTP`.
2. MPIN validation runs from the Render environment.
3. Existing Gist candles are already restored at process startup; after login the bot tries a historical gap fill only if necessary.
4. NIFTY 50 + SENSEX live WebSocket starts.
5. Automatic signal evaluation runs during **09:30–15:30 IST**.
6. A+/STRONG technical signals pass through the original option-quality gate and, if usable, Telegram receives option contract, live premium, OI/liquidity, Entry, SL, T1, T2 and confirmations.
7. Stocks remain OFF until `/stockon`.

## Important TOTP limitation

During a normal uninterrupted Render process, one morning TOTP is enough and ordinary
WebSocket disconnects are auto-reconnected. If Render destroys/restarts the entire process,
the in-memory authenticated Kotak Neo client is lost. The bot restores candles from Gist and
sends **RELOGIN REQUIRED**, but a fresh TOTP is still required. This build intentionally does
not store a TOTP seed or bypass 2FA.

## Original index strategy lock

Warm-up:
- 1m: 22 candles
- 5m: 21 candles
- 15m: 9 candles

Public classifications:
- A+ >= 80
- STRONG >= 70
- WATCH >= 60 (not actionable)

Original option quality gate:
- Option LTP > 0
- OI > 0
- Liquidity score >= 60

Original premium plan:
- SL = 15% below entry premium
- T1 = 1R
- T2 = 2R

## Telegram commands / buttons

- `/start` / `/help`
- `/status`
- `/login 123456`
- `/loginhelp`
- `/indexon` / `/indexoff`
- `/stockon` / `/stockoff`
- `/save`
- `/test`

## Required Render environment

See `env.example`. The key reliability items are:

```text
PUBLIC_URL=https://king-bro-4pn0.onrender.com
GITHUB_TOKEN=...
STATE_GIST_ID=...
STATE_GIST_FILENAME=kingbro_original_state.json
KINGBRO_KEEPALIVE_ENABLED=true
KINGBRO_SUPERVISOR_ENABLED=true
KINGBRO_HISTORICAL_BACKFILL=true
```

The GitHub token needs **Account permissions -> Gists -> Read and write**.
For an upgrade on the same Render URL, leave `BOOTSTRAP_URL` blank.

## GitHub Actions keepalive

Create repository secret:

```text
SERVICE_URL=https://king-bro-4pn0.onrender.com
```

Workflow: `.github/workflows/market-keepalive.yml`.
It gates requests to 09:00–15:40 IST on weekdays.

## Render

Build command:

```text
pip install -r requirements.txt
```

Start command:

```text
uvicorn main:app --host 0.0.0.0 --port $PORT
```

Keep **Auto-Deploy OFF** after the verified manual deploy so a GitHub commit does not
unexpectedly restart the live Kotak session during market hours.

## Validation

After deploy:

1. `/health` -> `ok: true`, `version: 7.0.0`, `persistence.configured: true`.
2. Telegram `/start`, then `/status`.
3. Morning `/login TOTP`.
4. `/status` should show `Broker: CONNECTED`, `Index feed: LIVE` and increasing/evaluating diagnostics.

This is a signal-analysis tool, not a performance guarantee. Trading decisions remain manual.
