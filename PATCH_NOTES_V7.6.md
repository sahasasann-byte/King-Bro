# KING BRO V7.6 — FINAL SIGNAL RELIABILITY + COMPACT UI

## Strategy lock
No strategy loosening was made.

Unchanged from V7.5:
- `_direction_score()` scoring rules and weights
- A+ threshold: 80+
- STRONG threshold: 70+
- WATCH/NO_TRADE behavior
- Existing option quality rule: usable LTP + OI > 0 + liquidity score >= 60
- Existing option risk/target model
- Telegram final-alert eligibility and duplicate cooldown

## 1. Missed strong option signal fix
V7.5 logs showed a real example:

`NIFTY 50 direction=PUT score=85 grade=OPTION_NOT_READY`

The technical setup was strong, but Kotak `quote_type=all` returned no usable LTP for the shortlisted option contract. V7.6 keeps `all` as the primary quote and adds targeted fallbacks only when a field is missing:

1. `all`
2. `ltp` only if LTP is absent
3. `oi` only if derivative OI is absent
4. `market_depth` only if two-sided depth is absent
5. legacy `depth` spelling only if needed/supported

Fallback payloads are merged without inventing data. The old LTP/OI/liquidity >= 60 filter still decides whether the signal is actionable.

Diagnostics now expose which quote types supplied a selected option via `quote_types`.

## 2. Post-close false feed restart fixed
V7.5 used 09:00–15:40 IST for both Render keepalive and feed-stale detection. That caused repeated `FEED_RESTART no index tick` after the normal 15:30 close.

V7.6 separates them:
- Feed watchdog: 09:15–15:30 IST
- Render keepalive/login buffer: 09:00–15:40 IST

So Render can remain awake for the operational buffer without treating normal post-close silence as a broken Kotak feed.

## 3. Real Market Sentiment reading
The old card showed `WAIT —/100` whenever no actionable signal existed.

V7.6 adds both bullish and bearish technical scores to each index signal payload and calculates a continuous combined NIFTY + SENSEX technical bias:
- 0 = strongest bearish bias
- 50 = neutral
- 100 = strongest bullish bias

The sentiment display is independent of the option confirmation stage, so an option-data issue cannot blank the market sentiment card. This is a display/diagnostic reading and does not change signal eligibility.

## 4. UI cleanup
- Duplicate top Market Overview price card removed; live NIFTY/SENSEX prices remain in the main signal workspace.
- Key Levels card is hidden until at least one real Daily/5M level set is available.
- Unavailable Key Level tab is disabled rather than showing meaningless dashes.
- Indicator cards/rows with no real reading are not rendered.
- Empty STOPPED Stock Scanner summary is hidden, but the Stock Scan START/STOP control remains permanently available in Scan Controls.
- If a technically strong setup is blocked by option data, the Scalping Signals panel now shows the blocker instead of only displaying WAITING.
- Layout automatically reflows when Key Levels is hidden.

## Verification performed
- Python compile: PASS
- Mock quote fallback where `all` lacked LTP: PASS
- Mock quote fallback where `all` lacked OI/depth: PASS
- Feed-session vs keepalive-window boundary tests: PASS
- `_direction_score()`: byte-for-source unchanged from V7.5
- `_attach_auto_option_to_signal()`: source unchanged from V7.5
- `_option_trade_plan()`: source unchanged from V7.5
- Telegram send/eligibility function: unchanged from V7.5
