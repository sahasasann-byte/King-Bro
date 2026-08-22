
# King Bro Terminal V3 — Live Quotes Only

This build removes TOTP/MPIN login and order trading.

It uses only:
- Kotak Neo `consumer_key`
- Kotak Neo `quotes()` API
- NIFTY 50
- SENSEX
- BANK NIFTY
- no mock fallback

## Render backend

Root Directory: `backend`

Build:
`pip install -r requirements.txt`

Start:
`uvicorn app.main:app --host 0.0.0.0 --port $PORT`

Environment:
- `KOTAK_CONSUMER_KEY` = your current Neo Consumer Key
- `KOTAK_ENVIRONMENT` = `prod`
- `FRONTEND_URL` = your frontend URL, for example `https://king-bro-1.onrender.com`

## Render frontend

Root Directory: `frontend`

Build:
`npm install && npm run build`

Publish:
`dist`

Environment:
- `VITE_API_URL` = `https://king-bro.onrender.com`

## Test

Backend health:
`https://king-bro.onrender.com/health`

Live quotes:
`https://king-bro.onrender.com/api/market/quotes`

If Kotak rejects the request, the real error is returned. The app never inserts a fake price.
