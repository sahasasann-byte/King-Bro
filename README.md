
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
