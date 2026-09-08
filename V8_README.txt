KING BRO V8 RELIABLE

Goals
- Preserve original index strategy and its 22x1m / 21x5m / 9x15m readiness gates.
- Telegram only; no automatic order execution.
- Preserve NIFTY/SENSEX + optional old 40-stock scanner.
- Persist every completed 5m candle to private Gist.
- Restore saved 1m/5m/15m directly after Render restart.
- After TOTP login, attempt official Kotak Neo 3.0.6 historical_data gap fill.
- Show the exact SDK historical_data signature/error in /status so failures are no longer hidden.
- Never invent/fake candles.

VERIFY AFTER DEPLOY
1. /login CURRENT_TOTP
2. wait 20-30 seconds
3. /status

Ideal:
Broker CONNECTED
Index feed LIVE
Historical preload attempted YES
Historical error None
NIFTY/SENSEX 5M >=21 and 15M >=9

If Historical error is not None, send the /status text. It now exposes the real SDK/API failure.
Even if historical REST is unavailable for indices, today's completed 5m/15m candles are force-saved,
so subsequent restarts/next session retain the warm-up instead of starting from zero.
