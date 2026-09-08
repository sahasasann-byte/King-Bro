# KING BRO V7.3 — HISTORY PRELOAD FIX

Purpose: remove the long same-day warm-up after a Render restart without weakening the original strategy.

Changes:
- Original index strategy thresholds/gates remain 22x1m / 21x5m / 9x15m.
- After TOTP login, Kotak historical data is used as a gap filler when Gist history is insufficient.
- Prefer completed real 1-minute historical candles, then rebuild exact 5m/15m OHLC from them.
- Direct 5m/15m historical calls are fallback only.
- No fake/synthetic candles.
- Gist remains the durable primary state.
- /status now exposes historical preload loaded/error diagnostics.
- Existing Telegram-only flow, stock ON/OFF and manual-order-only mode are preserved.

Expected after /login:
[HISTORY_READY_CHECK] NIFTY=1m:.../5m:21+/15m:9+ SENSEX=1m:.../5m:21+/15m:9+

If Kotak rejects historical data, /status will show the exact Historical error instead of silently waiting.
