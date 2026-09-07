# KING BRO V7 FINAL

Reliability release based on the user-supplied keepalive build, while preserving the original V7.3 signal rules.

- market-hours-only internal keepalive
- external GitHub Actions keepalive
- WebSocket supervisor and stale-feed recovery
- explicit re-login notification after auth/session loss
- Gist autosave + shutdown save + restore
- expanded 1m retention for previous-session daily levels
- best-effort Kotak 3.0.6 historical gap fill
- improved `/status` and `/health` diagnostics
- no automatic order execution
