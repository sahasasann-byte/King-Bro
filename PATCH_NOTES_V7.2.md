# KING BRO V7.2 — Restart History / Gist Reliability Fix

## What this fixes

1. **No more 12 PM warm-up caused only by a Render restart** when enough real 1-minute candles already exist in the restored Gist.
   - Missing 5-minute and 15-minute histories are reconstructed strictly from **completed real 1-minute OHLC candles**.
   - A 5m/15m bucket is accepted only if every required 1-minute candle exists at the exact consecutive timestamps.
   - No fake/interpolated candles are created.

2. **Original V7.3 signal strategy is unchanged.**
   - `direction_score`
   - `grade_for_score`
   - `evaluate_index_signal`
   - `option_trade_plan`
   - `evaluate_stock_signal`
   were AST-compared against V7.1 and are unchanged.

3. **Gist write pressure reduced.**
   - Auto-save default changed to 15 minutes.
   - Repeated close-boundary save calls are coalesced by a 240-second minimum write interval.
   - `/save`, historical backfill completion, bootstrap import, and shutdown can force a save.
   - This reduces the chance of GitHub secondary rate-limit 403 responses.

4. **Restore diagnostics improved.**
   Startup log now prints restored NIFTY/SENSEX 1m/5m/15m counts immediately.

5. **Historical backfill remains best-effort only.**
   After `/login TOTP`, Kotak historical data can fill real gaps if supported by the account/API; then higher timeframes are repaired from real 1m data and force-saved.

## Expected morning flow

- Start service / let GitHub Actions wake it.
- Send `/login CURRENT_TOTP` once.
- Gist restores previous candles.
- If restored 1m history contains enough complete minutes, 5m/15m are rebuilt immediately.
- From 09:30 IST, the unchanged V7.3 strategy evaluates normally.

## Important limitation

A true Render process restart destroys the in-memory Kotak authenticated session. Candle history is restored, but a fresh TOTP is still required after that restart. This cannot be safely bypassed.
