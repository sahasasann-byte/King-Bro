# V7 FINAL compare notes

Base reviewed: user-supplied `King-Bro-keepalive.zip` (Grok patch).

## Kept from the base
- Telegram webhook + persistent keyboard
- Original V7.3 index scoring engine
- Original 40-stock scanner
- Gist persistence
- Kotak option contract/premium enrichment
- Manual-only execution model
- `kotakneoapi==3.0.6`

## Fixed / added
1. Replaced 24-hour internal self-ping with market-window-only self-ping.
2. Added external GitHub Actions `/health` keepalive during 09:00–15:40 IST.
3. Added feed supervisor for dead/stale WebSocket recovery using the current authenticated session.
4. Added auth/session-expiry detection and Telegram re-login notice.
5. Added periodic Gist autosave and best-effort shutdown save.
6. Added Gist error de-duplication to avoid log flooding.
7. Expanded index 1m history to 900 candles so previous-session daily levels can survive the next session.
8. Added best-effort official `historical_data()` gap fill after login when Gist is insufficient.
9. Added actionable diagnostics to `/status` and `/health`.
10. Added explicit no-false-promise behavior: after a whole Render process restart, fresh TOTP may be required.

## Strategy deliberately NOT changed
- index warm-up 22/21/9
- A+ / STRONG thresholds
- V7.3 score weights
- option liquidity gate
- 15% premium SL, T1=1R, T2=2R
- stock strategy thresholds/universe
