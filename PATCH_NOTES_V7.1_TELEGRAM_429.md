# KING BRO V7.1 — Telegram 429 / Status Reply Fix

Only Telegram reliability/startup behavior was changed. Trading strategy thresholds and scoring remain unchanged.

Fixes:
- Serializes Telegram sends and enforces a 1.2s per-chat gap.
- Parses Telegram HTTP 429 response body and obeys `parameters.retry_after`.
- Retries rate-limited sends safely instead of blind 1.25s retries.
- Skips `setWebhook` when the correct webhook is already configured.
- Skips `setMyCommands` when commands are already correct; command setup 429 no longer breaks startup.
- Removes automatic generic startup message to avoid rolling-deploy flood bursts.
- Keeps one delayed market-hours `RELOGIN REQUIRED` notice after a real process restart.
- Adds browser-safe diagnostics endpoint: `/api/telegram/diag`.

If `/status` ever does not reply, open:
`https://king-bro-4pn0.onrender.com/api/telegram/diag`
It exposes webhook/pending/last Telegram error and last 429 retry time without exposing the bot token.
