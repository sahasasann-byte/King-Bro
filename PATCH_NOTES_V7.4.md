# KING BRO V7.4 — Render Market-Hours Reliability Patch

Scope: reliability only. Trading/signal strategy is unchanged.

## What changed
- Best-effort Render self-keepalive every 8 minutes, only after successful Kotak login and only 09:00–15:40 Asia/Kolkata.
- Uses Render's automatic `RENDER_EXTERNAL_URL`; no extra keepalive URL is normally required.
- Silent Kotak feed watchdog (default stale threshold 90 seconds) and automatic feed-task recovery.
- Ordinary websocket/network disconnects reconnect without asking for TOTP again.
- Only explicit auth-expiry style errors mark `KOTAK RELOGIN REQUIRED`.
- Index scanner is forced ON after successful morning Kotak login.
- Telegram delivery runs as a background task so Telegram latency/retries cannot block live Kotak tick consumption.
- Option discovery/quote confirmation has a 30-second I/O timeout to prevent an external API hang from freezing the feed loop. Existing option quality criteria are unchanged.
- Frontend no longer shows the TOTP form for a temporary feed reconnect; it asks for TOTP only when broker authentication is disconnected.
- `/health` includes runtime and Telegram diagnostics.

## Verified unchanged
- `_direction_score()` exact source unchanged.
- A+ threshold: score >= 80.
- STRONG threshold: score >= 70.
- Option liquidity pass threshold: >= 60, plus valid LTP and OI > 0.
- Telegram alert eligibility logic exact source unchanged: final actionable A+/STRONG only.
- 15-minute Telegram duplicate cooldown unchanged.
- Signal-only/manual-order model unchanged.

## Render expectation
Morning TOTP request wakes/activates the backend. While the authenticated session is active during market hours, the self-keepalive is intended to prevent Render's 15-minute idle spin-down even if the dashboard browser is closed. Render can still restart a Free instance at platform discretion; a true process restart cannot reuse a TOTP that was only held in memory and may require a fresh Kotak login.
