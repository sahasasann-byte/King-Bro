
import asyncio
from datetime import datetime, timezone
from typing import Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from neo_api_client import NeoAPI
from neo_api_client.websocket.feed import WsToken, SFeedScrip
from config import settings

app=FastAPI(title="King Bro Terminal API", version="4.0.0")
app.add_middleware(CORSMiddleware,allow_origins=[settings.FRONTEND_URL],allow_credentials=True,allow_methods=["*"],allow_headers=["*"])

class TotpRequest(BaseModel):
    totp:str=Field(min_length=6,max_length=6)

clients=set()
latest={}
status={"broker_connected":False,"feed_connected":False,"last_tick_at":None,"last_error":None,"mock_data":False}
neo:Optional[NeoAPI]=None
feed_task=None

async def broadcast(payload):
    dead=[]
    for ws in list(clients):
        try: await ws.send_json(payload)
        except: dead.append(ws)
    for ws in dead: clients.discard(ws)

async def set_status(**kw):
    status.update(kw)
    await broadcast({"type":"status","data":status})

def canonical(symbol):
    s=(symbol or "").lower()
    if "sensex" in s:return "SENSEX"
    if "nifty bank" in s or "bank nifty" in s or "banknifty" in s:return "BANK NIFTY"
    if "nifty 50" in s or s.strip()=="nifty":return "NIFTY 50"
    return symbol or "UNKNOWN"

def num(v):
    try:return float(str(v).replace(",","")) if v is not None else None
    except:return None

async def feed_loop():
    global neo
    backoff=2
    while neo is not None:
        try:
            async with neo.create_websocket() as ws:
                await ws.subscribe_scrips([
                    WsToken("nse_cm","Nifty 50"),
                    WsToken("nse_cm","Nifty Bank"),
                    WsToken("bse_cm","SENSEX"),
                ])
                await set_status(feed_connected=True,last_error=None)
                async for msg in ws:
                    if not isinstance(msg,SFeedScrip): continue
                    symbol=getattr(msg,"trading_symbol",None) or getattr(msg,"display_symbol",None) or str(getattr(msg,"instrument_token",""))
                    key=canonical(symbol)
                    if key not in ("NIFTY 50","SENSEX","BANK NIFTY"): continue
                    now=datetime.now(timezone.utc).isoformat()
                    item={"key":key,"symbol":symbol,"ltp":num(getattr(msg,"last_traded_price",None)),"change":num(getattr(msg,"change",None)),"percent_change":num(getattr(msg,"percentage_change",None) or getattr(msg,"percent_change",None) or getattr(msg,"per_change",None)),"received_at":now}
                    latest[key]=item
                    await set_status(last_tick_at=now)
                    await broadcast({"type":"tick","data":item})
                backoff=2
        except asyncio.CancelledError:return
        except Exception as e:
            await set_status(feed_connected=False,last_error=f"{type(e).__name__}: {e}")
            await asyncio.sleep(backoff)
            backoff=min(backoff*2,30)

@app.get("/")
async def root():
    return {"app":"King Bro Terminal","status":"online","mock_data":False}

@app.get("/health")
async def health():
    return {"status":"ok",**status,"configured":{"consumer_key":bool(settings.KOTAK_CONSUMER_KEY),"mobile":bool(settings.KOTAK_MOBILE_NUMBER),"ucc":bool(settings.KOTAK_UCC),"mpin":bool(settings.KOTAK_MPIN)}}

@app.post("/api/kotak/connect")
async def connect(body:TotpRequest):
    global neo,feed_task
    missing=[k for k,v in {"KOTAK_CONSUMER_KEY":settings.KOTAK_CONSUMER_KEY,"KOTAK_MOBILE_NUMBER":settings.KOTAK_MOBILE_NUMBER,"KOTAK_UCC":settings.KOTAK_UCC,"KOTAK_MPIN":settings.KOTAK_MPIN}.items() if not v]
    if missing: raise HTTPException(400,detail="Missing Render variables: "+", ".join(missing))
    def auth():
        c=NeoAPI(consumer_key=settings.KOTAK_CONSUMER_KEY,environment=settings.KOTAK_ENVIRONMENT)
        c.totp_login(mobile_number=settings.KOTAK_MOBILE_NUMBER,ucc=settings.KOTAK_UCC,totp=body.totp)
        c.totp_validate(mpin=settings.KOTAK_MPIN)
        return c
    try:
        neo=await asyncio.to_thread(auth)
        await set_status(broker_connected=True,feed_connected=False,last_error=None)
        if feed_task and not feed_task.done(): feed_task.cancel()
        feed_task=asyncio.create_task(feed_loop())
        return {"ok":True,"message":"Kotak authenticated. Live feed starting."}
    except Exception as e:
        await set_status(broker_connected=False,feed_connected=False,last_error=f"{type(e).__name__}: {e}")
        raise HTTPException(400,detail=f"{type(e).__name__}: {e}")

@app.get("/api/market/snapshot")
async def snapshot():
    return {"status":status,"items":list(latest.values())}

@app.websocket("/ws/market")
async def browser_ws(ws:WebSocket):
    await ws.accept();clients.add(ws)
    await ws.send_json({"type":"status","data":status})
    if latest:await ws.send_json({"type":"snapshot","data":list(latest.values())})
    try:
        while True:await ws.receive_text()
    except WebSocketDisconnect:clients.discard(ws)
    except Exception:clients.discard(ws)
