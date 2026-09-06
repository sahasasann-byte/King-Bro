# KING BRO Telegram Original V6 — Daily Auto

Telegram-only, manual-order signal service for the user's original V7.3 multi-timeframe strategy.

## Daily behaviour

1. Morning: send `/login CURRENT_6_DIGIT_TOTP` once.
2. Bot validates Kotak login with MPIN from Render env.
3. NIFTY 50 + SENSEX live feed starts automatically.
4. Automatic signal generation runs during **09:30–15:30 IST**.
5. A+/STRONG signals are enriched with the option contract, live option premium, OI/liquidity, Entry, SL, T1 and T2 and sent to Telegram.
6. Order placement remains **MANUAL ONLY**. This project contains no auto-order execution.
7. Stock scanner is **OFF by default**. Use `/stockon` and `/stockoff`.

## Original index strategy lock

Warm-up requirements are unchanged:
- 1-minute candles: 22
- 5-minute candles: 21
- 15-minute candles: 9

Signal score thresholds:
- A+ >= 80
- STRONG >= 70
- WATCH >= 60 (not actionable)

Option quality gate is preserved. Premium risk plan is the old 15% SL, T1=1R, T2=2R.

## Important: first deployment / empty Gist

This build restores candle history from a **private GitHub Gist**. Render Free local `/tmp` is not durable.

If the Gist already contains a prior live session, the restored candles can make the original engine ready from the morning without waiting for 21 new 5-minute candles.

If the Gist is empty, the first live session has to build the required candles from the live feed. Once saved, later Render restarts/mornings restore them from Gist.

Kotak's own public support page currently says historical-data retrieval is not allowed for Neo Trade API, so this build does **not** pretend that a guaranteed Kotak historical backfill is available. It uses real saved live candles instead.

## Telegram commands

- `/start` or `/help` — menu/help
- `/status` — broker/feed/readiness status
- `/login 123456` — morning TOTP login (message deletion attempted immediately)
- `/loginhelp` — login help
- `/indexon` / `/indexoff`
- `/stockon` / `/stockoff`
- `/save` — save candle state to private Gist
- `/test` — Telegram service test

## Required Render environment variables

```text
KOTAK_CONSUMER_KEY=
KOTAK_MOBILE_NUMBER=
KOTAK_UCC=
KOTAK_MPIN=
KOTAK_ENVIRONMENT=prod

TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
TELEGRAM_WEBHOOK_SECRET=use-a-long-random-secret
PUBLIC_URL=

GITHUB_TOKEN=
STATE_GIST_ID=
STATE_GIST_FILENAME=kingbro_original_state.json

# Leave blank when replacing the existing service at the same URL.
BOOTSTRAP_URL=
```

Render automatically supplies `RENDER_EXTERNAL_URL`, so `PUBLIC_URL` can normally be blank. If you set it manually, use your exact Render service URL.

## Same old Render service

You can replace the code in the existing `king-bro` GitHub repo and keep the same Render service. Keep Auto-Deploy OFF while changing files; deploy once manually after env values are correct.

Do not set `BOOTSTRAP_URL` to the same service URL you are replacing.
