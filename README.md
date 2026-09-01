
# King Bro Terminal V4

Screen shows only the current TOTP. Consumer Key, Mobile Number, UCC and MPIN stay in Render Environment Variables.

Backend Render:
- Root Directory: backend
- Build: pip install -r requirements.txt
- Start: uvicorn app.main:app --host 0.0.0.0 --port $PORT

Backend Environment:
- KOTAK_CONSUMER_KEY
- KOTAK_MOBILE_NUMBER
- KOTAK_UCC
- KOTAK_MPIN
- KOTAK_ENVIRONMENT=prod
- FRONTEND_URL=https://YOUR-FRONTEND.onrender.com
- PYTHON_VERSION=3.12.10

Frontend Render Static Site:
- Root Directory: frontend
- Build: npm install && npm run build
- Publish Directory: dist
- VITE_API_URL=https://YOUR-BACKEND.onrender.com

No mock data. No order placement.

## V7.3 Light-mode patch
- Trading/signal strategy is unchanged.
- WebSocket stays the primary live data path.
- Frontend REST safety refresh reduced from 30s to 60s.
- Heavy signal-detail refresh reduced to 120s.
- REST refresh pauses while browser tab is hidden.
- Positions refresh reduced from 30s to 60s.
- Telegram env keys added to env.example.
- Telegram test endpoint: POST /api/telegram/test
- Telegram status endpoint: GET /api/telegram/status

### LIGHT Telegram Check
Dashboard LIVE FEED card includes a manual **TELEGRAM CHECK** button. It does not poll in the background. A tap first checks `/api/telegram/status`, then sends one test message through `/api/telegram/test`. Strategy/signal logic is unchanged.
