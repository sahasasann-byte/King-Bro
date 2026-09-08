# KING BRO V7.1 — Telegram 429 / Status Delivery Fix

Only Telegram delivery/setup reliability is changed. Original V7.3 strategy, candle requirements, score thresholds, option gate, targets, stocks and manual-only execution remain unchanged.

Fixes:
- Serializes Telegram sends to the private chat (minimum 1.10s gap).
- Parses Telegram HTTP 429 response and obeys `parameters.retry_after` instead of retrying blindly every 1.25s.
- Retries outbound messages up to 5 attempts.
- Makes webhook setup idempotent: if the correct webhook is already set, it is not set again on every Render restart.
- Makes command-menu refresh non-critical.
- Removes the routine backend-ready startup message, reducing restart message bursts.
- Keeps the market-hours RELOGIN REQUIRED notice because it requires user action.

No trading strategy logic was modified.
