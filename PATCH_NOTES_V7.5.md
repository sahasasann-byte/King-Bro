# KING BRO V7.5 — Render restart diagnostics

Reliability-only patch. Trading strategy, scores, thresholds, option filters and Telegram signal eligibility are unchanged.

## What this fixes
- Makes Render process restarts explicit: dashboard shows `SERVER RESTARTED / TOTP LOGIN REQUIRED` instead of looking like an unexplained logout.
- Sends one Telegram operational warning when a Render process starts during market hours and Kotak must be logged in again.
- Render self-keepalive now continues during market hours even after a process restart cleared the in-memory broker session.
- Keepalive interval default reduced from 8 minutes to 5 minutes.
- `/health` now exposes boot id/start time, last tick age, restart-login state, and signal diagnostics.
- Every NIFTY/SENSEX candle evaluation logs `[SIGNAL_EVAL]` with direction, score, final grade, actionable flag and blockers.
- Signal history compares the final post-option-filter grade, fixing a history/diagnostic mismatch only.

## Important limit
A Render process restart destroys the in-memory `NeoAPI` authenticated object. A 6-digit TOTP itself cannot be reused because it expires. This patch does **not** store or bypass 2FA. After an actual Render restart, one fresh TOTP is still required before live Kotak signals can resume.
