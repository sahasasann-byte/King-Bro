
# King Bro Terminal V4

Screen shows only the current TOTP. Consumer Key, Mobile Number, UCC and MPIN stay in Render Environment Variables.

Backend Render:
- Root Directory: backend
- Build: pip install -r requirements.txt
- Start: uvicorn app.main:app --host 0.0.0.0 --port $PORT

Backend Environment:
- KOTAK_CONSUMER_KEY
- KOTAK_MOBILE_NUMBER
- KOTAK_UCC
- KOTAK_MPIN
- KOTAK_ENVIRONMENT=prod
- FRONTEND_URL=https://YOUR-FRONTEND.onrender.com
- PYTHON_VERSION=3.12.10

Frontend Render Static Site:
- Root Directory: frontend
- Build: npm install && npm run build
- Publish Directory: dist
- VITE_API_URL=https://YOUR-BACKEND.onrender.com

No mock data. No order placement.

## V7.3 Light-mode patch
- Trading/signal strategy is unchanged.
- WebSocket stays the primary live data path.
- Frontend REST safety refresh reduced from 30s to 60s.
- Heavy signal-detail refresh reduced to 120s.
- REST refresh pauses while browser tab is hidden.
- Positions refresh reduced from 30s to 60s.
- Telegram env keys added to env.example.
- Telegram test endpoint: POST /api/telegram/test
- Telegram status endpoint: GET /api/telegram/status

### LIGHT Telegram Check
Dashboard LIVE FEED card includes a manual **TELEGRAM CHECK** button. It does not poll in the background. A tap first checks `/api/telegram/status`, then sends one test message through `/api/telegram/test`. Strategy/signal logic is unchanged.


## V7.4 Render market-hours reliability patch (strategy unchanged)

Goal: one morning Kotak TOTP, then keep the NIFTY 50 + SENSEX signal engine and Telegram delivery alive through the Indian market session even when the dashboard/browser is closed.

Reliability changes only:
- Signal scoring, A+/STRONG thresholds, option LTP/OI/liquidity filter, SL/targets and Telegram alert eligibility are unchanged.
- Index scanner is automatically ON after a successful morning Kotak login.
- Kotak websocket reconnects automatically on ordinary disconnects.
- A market-hours supervisor restarts a silently stale feed (default: no index tick for 90 seconds).
- A real auth-expiry style error is the only feed error that marks `KOTAK RELOGIN REQUIRED`.
- Telegram sending runs outside the market-feed read loop so a slow Telegram request cannot block Kotak ticks.
- Option confirmation has a 30-second I/O timeout so a stuck Kotak quote/search request cannot freeze the feed loop indefinitely. The option filter itself is unchanged.
- During 09:00–15:40 Asia/Kolkata, after successful broker login, the backend makes a best-effort self keepalive request every 8 minutes using Render's `RENDER_EXTERNAL_URL`. This is intended to prevent inactivity spin-down while the signal engine is needed; browser may stay closed.
- `/health` now exposes `runtime` and `telegram` diagnostics.
- Temporary feed disconnect no longer forces the frontend to show the TOTP box; it shows login only when broker authentication is actually not connected.

Optional environment overrides (defaults are already in code):
- `KINGBRO_KEEPALIVE_ENABLED=true`
- `KINGBRO_KEEPALIVE_INTERVAL_SECONDS=300`
- `KINGBRO_FEED_STALE_SECONDS=90`
- `KINGBRO_FEED_RESTART_COOLDOWN_SECONDS=60`
- `KINGBRO_OPTION_CONFIRM_TIMEOUT_SECONDS=30`
- `KINGBRO_KEEPALIVE_URL=` (leave blank on Render unless `RENDER_EXTERNAL_URL` is unavailable)

Operational expectation:
1. Open KING BRO in the morning and submit the current Kotak TOTP once.
2. Confirm `/health` shows `broker_connected: true`, `feed_connected: true`, `runtime.supervisor_running: true` and `runtime.keepalive_url_detected: true`.
3. Dashboard/browser may then be closed; actionable A+/STRONG signals continue to use the same Telegram rules.
4. A broker session that genuinely expires still requires a fresh TOTP; the patch does not bypass Kotak authentication.
