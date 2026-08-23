import React from "react";
import { createRoot } from "react-dom/client";
import {
  Crown, Home, Activity, Crosshair, BellRing, BriefcaseBusiness,
  Settings, Play, Square, Wifi, WifiOff, Zap, RefreshCw, X,
  BarChart3, ShieldCheck, ScanLine, Radio, ChevronRight
} from "lucide-react";
import "./styles.css";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";
const WS_URL = API.replace(/^http/, "ws") + "/ws/market";

const EMPTY = {
  "NIFTY 50": { key: "NIFTY 50", ltp: null },
  "SENSEX": { key: "SENSEX", ltp: null },
  "BANK NIFTY": { key: "BANK NIFTY", ltp: null },
};

function n(v, digits = 2) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "—";
  return Number(v).toLocaleString("en-IN", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function MarketMini({ item }) {
  const ch = Number(item?.change || 0);
  const up = ch >= 0;
  return (
    <div className="mini-market glass-card">
      <div className="mini-label">{item.key}</div>
      <div className="mini-ltp">{n(item.ltp)}</div>
      <div className={`mini-change ${up ? "pos" : "neg"}`}>
        {item.change == null ? "Waiting for live tick" : `${up ? "+" : ""}${n(ch)}`}
      </div>
      <div className="spark">
        <span />
        <span />
        <span />
        <span />
        <span />
      </div>
    </div>
  );
}

function CandleChart({ candles = [], latestLtp }) {
  const data = candles.slice(-42);
  if (!data.length) {
    return (
      <div className="chart-empty">
        Waiting for 1-minute candles…
      </div>
    );
  }

  const width = 760;
  const height = 330;
  const pad = 24;
  const lows = data.map(c => Number(c.low));
  const highs = data.map(c => Number(c.high));
  const lo = Math.min(...lows);
  const hi = Math.max(...highs);
  const range = Math.max(hi - lo, 1);
  const cw = (width - pad * 2) / data.length;
  const y = (p) => pad + (hi - p) / range * (height - pad * 2);

  const closePoints = data.map((c, i) => {
    const x = pad + i * cw + cw / 2;
    return `${x},${y(Number(c.close))}`;
  }).join(" ");

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="chart-svg" preserveAspectRatio="none">
      <defs>
        <linearGradient id="lineGlow" x1="0" x2="1">
          <stop offset="0%" stopColor="#00c7ff" />
          <stop offset="100%" stopColor="#0cff8f" />
        </linearGradient>
      </defs>

      {[0.2,0.4,0.6,0.8].map((r) => (
        <line key={r}
          x1={pad} x2={width-pad}
          y1={pad + (height-pad*2)*r}
          y2={pad + (height-pad*2)*r}
          stroke="rgba(130,205,255,.10)" strokeWidth="1"
        />
      ))}

      {data.map((c, i) => {
        const open = Number(c.open), close = Number(c.close);
        const high = Number(c.high), low = Number(c.low);
        const x = pad + i * cw + cw / 2;
        const bullish = close >= open;
        const color = bullish ? "#11ef93" : "#ff5d73";
        const bodyTop = y(Math.max(open, close));
        const bodyBottom = y(Math.min(open, close));
        return (
          <g key={i}>
            <line x1={x} x2={x} y1={y(high)} y2={y(low)} stroke={color} strokeWidth="1.2" opacity=".95" />
            <rect
              x={x - Math.max(cw * .23, 1.3)}
              y={bodyTop}
              width={Math.max(cw * .46, 2.6)}
              height={Math.max(bodyBottom - bodyTop, 2)}
              rx="1"
              fill={color}
            />
          </g>
        );
      })}

      <polyline
        points={closePoints}
        fill="none"
        stroke="url(#lineGlow)"
        strokeWidth="1.15"
        opacity=".45"
      />

      {latestLtp != null && (
        <g>
          <line x1={pad} x2={width-pad} y1={y(Number(latestLtp))} y2={y(Number(latestLtp))}
            stroke="#29ffc6" strokeDasharray="4 4" opacity=".65" />
          <rect x={width-104} y={Math.max(8,y(Number(latestLtp))-11)} width="82" height="22" rx="5"
            fill="#0bbf87" />
          <text x={width-63} y={Math.max(23,y(Number(latestLtp))+4)} textAnchor="middle"
            fontSize="10" fill="white" fontWeight="700">
            {n(latestLtp)}
          </text>
        </g>
      )}
    </svg>
  );
}

function ScannerControl({ scanners, busy, action }) {
  const indexRunning = !!scanners?.index?.enabled;
  const stockRunning = !!scanners?.stocks?.running;

  const Row = ({ label, sub, running, startPath, stopPath, keyName }) => (
    <div className="scan-row">
      <div>
        <b>{label}</b>
        <small>{sub}</small>
      </div>
      <div className={`toggle-visual ${running ? "on" : ""}`}><i /></div>
      <button className="scan-start" disabled={busy===`${keyName}-start`}
        onClick={() => action(startPath, `${keyName}-start`)}>
        <Play size={13}/> START
      </button>
      <button className="scan-stop" disabled={busy===`${keyName}-stop`}
        onClick={() => action(stopPath, `${keyName}-stop`)}>
        <Square size={12}/> STOP
      </button>
    </div>
  );

  return (
    <div className="scan-control glass-card">
      <div className="box-title">SCAN CONTROLS</div>
      <Row label="NIFTY + SENSEX" sub="Index signal engine" running={indexRunning}
        startPath="/api/scanners/index/start" stopPath="/api/scanners/index/stop" keyName="index" />
      <Row label="STOCK SCAN" sub="Fixed 40-stock universe" running={stockRunning}
        startPath="/api/scanners/stocks/start" stopPath="/api/scanners/stocks/stop" keyName="stock" />
      <div className="scan-stats">
        <span>Universe <b>40</b></span>
        <span>Resolved <b>{scanners?.stocks?.resolved ?? 0}</b></span>
        <span>Status <b className={stockRunning ? "pos" : "neg"}>{stockRunning ? "LIVE" : "STOP"}</b></span>
      </div>
    </div>
  );
}

function SentimentGauge({ signals }) {
  const scores = Object.values(signals).filter(Boolean).map(s => Number(s.score || 0));
  const avg = scores.length ? Math.round(scores.reduce((a,b)=>a+b,0)/scores.length) : 0;
  const dir = Object.values(signals).find(Boolean)?.direction || "WAIT";
  return (
    <div className="sentiment glass-card">
      <div className="box-title">MARKET SENTIMENT</div>
      <div className="gauge">
        <div className="gauge-inner">
          <span>{dir === "CALL" ? "BULLISH" : dir === "PUT" ? "BEARISH" : "WAIT"}</span>
          <b>{avg || "—"}</b>
          <small>/100</small>
        </div>
      </div>
    </div>
  );
}

function KeyLevels({ snapshot }) {
  const d = snapshot?.indicators?.daily || {};
  const f = d?.fib || d?.fibonacci || {};
  return (
    <div className="key-levels glass-card">
      <div className="box-title">KEY LEVELS</div>
      <div className="tabs-lite"><b>Daily</b><span>5 Min</span></div>
      <div className="levels-list">
        <span><em>R2</em><b>{n(d.r2)}</b></span>
        <span><em>R1</em><b>{n(d.r1)}</b></span>
        <span><em>P</em><b>{n(d.pivot)}</b></span>
        <span><em>S1</em><b>{n(d.s1)}</b></span>
        <span><em>S2</em><b>{n(d.s2)}</b></span>
      </div>
      <div className="fib-title">FIB LEVELS</div>
      <div className="fib-list">
        <span>0.382 <b>{n(f.r1 || f["0.382"])}</b></span>
        <span>0.618 <b>{n(f.r2 || f["0.618"])}</b></span>
        <span>1.000 <b>{n(f.r3 || f["1.000"])}</b></span>
      </div>
    </div>
  );
}

function ScalpingSignal({ signal, onOrder }) {
  const call = signal?.direction === "CALL";
  const put = signal?.direction === "PUT";
  const colorClass = call ? "call" : put ? "put" : "wait";

  return (
    <div className={`scalp-panel glass-card ${colorClass}`}>
      <div className="scalp-top">
        <span><Zap size={18}/> SCALPING SIGNAL</span>
        <b>{signal?.grade || "SCANNING"}</b>
      </div>

      <div className="scalp-symbol">
        {signal?.option_contract?.display_symbol ||
         signal?.option_contract?.trading_symbol ||
         "WAITING FOR SETUP"}
      </div>

      <div className="scalp-price">
        <small>Live LTP</small>
        <strong>{signal?.option_ltp == null ? "—" : `₹${n(signal.option_ltp)}`}</strong>
      </div>

      <div className="scalp-grid">
        <span>Entry <b>{signal?.entry == null ? "—" : `₹${n(signal.entry)}`}</b></span>
        <span>Stop Loss <b className="neg">{signal?.stop_loss == null ? "—" : `₹${n(signal.stop_loss)}`}</b></span>
        <span>Target 1 <b className="pos">{signal?.target_1 == null ? "—" : `₹${n(signal.target_1)}`}</b></span>
        <span>Target 2 <b className="pos">{signal?.target_2 == null ? "—" : `₹${n(signal.target_2)}`}</b></span>
      </div>

      <div className="score-ring">
        <small>SCORE</small>
        <b>{signal?.score ?? "—"}</b>
        <span>/100</span>
      </div>

      <div className="confirm-box">
        <div className="confirm-title">CONFIRMATION</div>
        <div className="confirm-grid">
          {(signal?.reasons || []).slice(0,8).map((r, i) => <span key={i}>✓ {r}</span>)}
          {!(signal?.reasons || []).length && <span>Waiting for confirmations…</span>}
        </div>
      </div>

      <button
        className="strong-action"
        disabled={!signal?.actionable || !signal?.option_contract}
        onClick={() => onOrder({ kind:"INDEX_OPTION", signal })}
      >
        {call ? "STRONG CALL SETUP" : put ? "STRONG PUT SETUP" : "WAITING"} <ChevronRight size={18}/>
      </button>
    </div>
  );
}

function IndicatorStrip({ snapshot }) {
  const one = snapshot?.indicators?.one_minute || {};
  const five = snapshot?.indicators?.five_minute || {};
  const fifteen = snapshot?.indicators?.fifteen_minute || {};
  const blocks = [
    ["1M INDICATORS", [
      ["RSI (14)", one.rsi14],
      ["Williams %R", one.williams_r14],
      ["EMA 9", one.ema9],
      ["EMA 21", one.ema21],
    ]],
    ["5M MOVING AVG", [
      ["EMA 9", five.ema9],
      ["EMA 21", five.ema21],
      ["MA 20", five.ma20],
      ["RSI", five.rsi14],
    ]],
    ["15M TREND", [
      ["EMA 9", fifteen.ema9],
      ["EMA 21", fifteen.ema21],
      ["RSI", fifteen.rsi14],
      ["Breakout", fifteen.breakout],
    ]],
    ["PRICE ACTION", [
      ["1M", one.price_action],
      ["5M", five.price_action],
      ["15M", fifteen.price_action],
      ["ATR", one.atr14],
    ]],
  ];
  return (
    <div className="indicator-strip">
      {blocks.map(([title, rows]) => (
        <div className="indicator-card glass-card" key={title}>
          <div className="box-title">{title}</div>
          {rows.map(([k,v]) => (
            <div className="indicator-row" key={k}>
              <span>{k}</span>
              <b>{typeof v === "number" ? n(v) : (v ?? "—")}</b>
              <i className={String(v).includes("BEAR") ? "red-dot":"green-dot"} />
            </div>
          ))}
        </div>
      ))}
    </div>
  );
}

function StockTable({ items, onOrder }) {
  return (
    <div className="bottom-card glass-card">
      <div className="box-title">TOP STOCK SCANS</div>
      <div className="table-head">
        <span>#</span><span>Stock</span><span>Trend</span><span>Score</span><span>Entry</span><span>Signal</span>
      </div>
      {(items || []).slice(0,4).map((s,i) => (
        <div className="table-row" key={s.symbol}>
          <span>{i+1}</span>
          <b>{s.symbol}</b>
          <span className={s.direction==="BUY" ? "pos":"neg"}>{s.direction}</span>
          <span>{s.score ?? "—"}</span>
          <span>{s.entry == null ? "—" : `₹${n(s.entry)}`}</span>
          <button disabled={!s.actionable} onClick={()=>onOrder({kind:"STOCK", signal:s})}>
            {s.direction || "WAIT"}
          </button>
        </div>
      ))}
      {!(items || []).length && <div className="bottom-empty">Waiting for stock signals…</div>}
    </div>
  );
}

function RecentSignals({ history }) {
  return (
    <div className="bottom-card glass-card">
      <div className="box-title">RECENT SIGNALS</div>
      {(history || []).slice(0,5).map((s,i) => (
        <div className="recent-row" key={i}>
          <span>{s.generated_at ? new Date(s.generated_at).toLocaleTimeString("en-IN",{hour:"2-digit",minute:"2-digit"}) : "—"}</span>
          <b>{s.symbol}</b>
          <span className={s.direction==="CALL"||s.direction==="BUY" ? "pos":"neg"}>{s.direction}</span>
          <span>{s.grade || "—"}</span>
          <span>{s.score ?? "—"}</span>
        </div>
      ))}
      {!(history || []).length && <div className="bottom-empty">No recent signals yet.</div>}
    </div>
  );
}

function Positions({ positions, loading, refresh, squareOff, squareOffAll }) {
  return (
    <div className="positions-box glass-card">
      <div className="positions-head">
        <div><div className="box-title">POSITIONS</div><small>Actual broker positions</small></div>
        <div>
          <button onClick={refresh}><RefreshCw size={14}/></button>
          <button className="sq-all" disabled={!positions.length} onClick={squareOffAll}>SQUARE OFF ALL</button>
        </div>
      </div>
      {loading ? <div className="bottom-empty">Loading…</div> :
       !positions.length ? <div className="bottom-empty">No open positions.</div> :
       positions.slice(0,4).map(p => (
        <div className="position-row" key={`${p.exchange_segment}-${p.trading_symbol}`}>
          <b>{p.trading_symbol}</b>
          <span>{p.side} • Qty {p.net_quantity}</span>
          <span>Avg ₹{n(p.average_price)}</span>
          <span className={Number(p.unrealized_pnl)>=0 ? "pos":"neg"}>
            {p.unrealized_pnl == null ? "P&L —" : `P&L ₹${n(p.unrealized_pnl)}`}
          </span>
          <button onClick={()=>squareOff(p)}>SQUARE OFF</button>
        </div>
      ))}
    </div>
  );
}

function ManualOrderModal({ draft, busy, close, submit }) {
  const [qty,setQty] = React.useState(String(draft?.quantity || 1));
  const [type,setType] = React.useState("MKT");
  const [price,setPrice] = React.useState("");

  React.useEffect(()=>{
    setQty(String(draft?.quantity || 1));
    setType("MKT");
    setPrice("");
  },[draft]);

  if (!draft) return null;

  const go = () => {
    const q = Number(qty);
    if (!Number.isInteger(q) || q<=0) return alert("Enter valid quantity");
    if (type==="L" && Number(price)<=0) return alert("Enter valid limit price");
    if (!window.confirm(`${draft.side==="B"?"BUY":"SELL"} ${q} ${draft.trading_symbol}?\n\nThis is a REAL manual order.`)) return;
    submit({...draft, quantity:q, order_type:type, price:type==="L"?Number(price):0});
  };

  return (
    <div className="modal-bg">
      <div className="order-modal">
        <button className="modal-x" onClick={close}><X size={18}/></button>
        <small>MANUAL ONLY</small>
        <h2>Place Order</h2>
        <div className="order-symbol"><b>{draft.side==="B"?"BUY":"SELL"}</b><span>{draft.trading_symbol}</span></div>
        <div className="order-form-grid">
          <label>Quantity<input type="number" min="1" value={qty} onChange={e=>setQty(e.target.value)}/></label>
          <label>Order Type<select value={type} onChange={e=>setType(e.target.value)}><option value="MKT">Market</option><option value="L">Limit</option></select></label>
        </div>
        {type==="L" && <label className="full-label">Limit Price<input type="number" step=".05" value={price} onChange={e=>setPrice(e.target.value)}/></label>}
        <button className="confirm-order" disabled={busy} onClick={go}>{busy?"PLACING...":"CONFIRM REAL ORDER"}</button>
      </div>
    </div>
  );
}

function App() {
  const [feed,setFeed] = React.useState(EMPTY);
  const [status,setStatus] = React.useState({broker_connected:false,feed_connected:false,last_error:null});
  const [totp,setTotp] = React.useState("");
  const [busy,setBusy] = React.useState(false);
  const [loginError,setLoginError] = React.useState("");
  const [message,setMessage] = React.useState("");
  const [scanners,setScanners] = React.useState({index:{enabled:true},stocks:{enabled:false,running:false}});
  const [scannerBusy,setScannerBusy] = React.useState("");
  const [signals,setSignals] = React.useState({"NIFTY 50":null,"SENSEX":null});
  const [history,setHistory] = React.useState([]);
  const [niftySnapshot,setNiftySnapshot] = React.useState(null);
  const [stockSignals,setStockSignals] = React.useState([]);
  const [positions,setPositions] = React.useState([]);
  const [positionsLoading,setPositionsLoading] = React.useState(false);
  const [orderDraft,setOrderDraft] = React.useState(null);
  const [orderBusy,setOrderBusy] = React.useState(false);

  const loadScanners = React.useCallback(async()=>{
    try {
      let r = await fetch(API+"/api/scanners/status");
      if(!r.ok) r = await fetch(API+"/api/scanners");
      if(r.ok) setScanners(await r.json());
    } catch {}
  },[]);

  const loadSignals = React.useCallback(async()=>{
    try {
      const r = await fetch(API+"/api/signals");
      if(!r.ok) return;
      const d = await r.json();
      const s = d?.signals || {};
      setSignals({"NIFTY 50":s["NIFTY 50"]||null,"SENSEX":s["SENSEX"]||null});
      setHistory(Array.isArray(d?.history)?d.history:[]);
    } catch {}
  },[]);

  const loadNiftySnapshot = React.useCallback(async()=>{
    try {
      const r = await fetch(API+"/api/signals/NIFTY");
      if(r.ok) setNiftySnapshot(await r.json());
    } catch {}
  },[]);

  const loadStocks = React.useCallback(async()=>{
    try {
      const r = await fetch(API+"/api/stocks/signals/best?limit=8");
      if(r.ok) {
        const d = await r.json();
        setStockSignals(Array.isArray(d?.items)?d.items:[]);
      }
    } catch {}
  },[]);

  const loadPositions = React.useCallback(async()=>{
    if(!status.broker_connected){ setPositions([]); return; }
    setPositionsLoading(true);
    try{
      const r=await fetch(API+"/api/positions");
      const d=await r.json();
      if(!r.ok) throw new Error(typeof d?.detail==="string"?d.detail:"Positions fetch failed");
      setPositions(Array.isArray(d?.items)?d.items:[]);
    } catch(e){ setMessage(`Positions: ${e?.message||e}`); }
    finally{ setPositionsLoading(false); }
  },[status.broker_connected]);

  React.useEffect(()=>{
    loadScanners(); loadSignals(); loadStocks(); loadNiftySnapshot();
    const t=setInterval(()=>{loadScanners();loadSignals();loadStocks();loadNiftySnapshot();},12000);
    return()=>clearInterval(t);
  },[loadScanners,loadSignals,loadStocks,loadNiftySnapshot]);

  React.useEffect(()=>{
    if(!status.broker_connected) return;
    loadPositions();
    const t=setInterval(loadPositions,15000);
    return()=>clearInterval(t);
  },[status.broker_connected,loadPositions]);

  React.useEffect(()=>{
    let ws, retry, ping, dead=false;
    const connect=()=>{
      if(dead) return;
      try{ ws=new WebSocket(WS_URL); }catch{ retry=setTimeout(connect,3000); return; }
      ws.onopen=()=>{
        try{ws.send("hello")}catch{}
        clearInterval(ping);
        ping=setInterval(()=>{if(ws?.readyState===WebSocket.OPEN){try{ws.send("ping")}catch{}}},20000);
      };
      ws.onmessage=(e)=>{
        try{
          const m=JSON.parse(e.data);
          if(m.type==="status") setStatus(m.data||{});
          if(m.type==="snapshot"){
            const next={...EMPTY};
            (Array.isArray(m.data)?m.data:[]).forEach(x=>{if(x?.key&&next[x.key])next[x.key]=x});
            setFeed(next);
          }
          if(m.type==="tick"){
            const x=m.data;
            if(x?.key&&Object.prototype.hasOwnProperty.call(EMPTY,x.key)) setFeed(p=>({...p,[x.key]:x}));
          }
          if(m.type==="signal_update"||m.type==="signal_event"){
            const s=m.data;
            if(s?.symbol==="NIFTY 50"||s?.symbol==="SENSEX") setSignals(p=>({...p,[s.symbol]:s}));
          }
          if(m.type==="stock_signal_update"){
            const s=m.data;
            if(s?.symbol) setStockSignals(prev=>{
              const next=[s,...prev.filter(x=>x.symbol!==s.symbol)];
              next.sort((a,b)=>Number(Boolean(b.actionable))-Number(Boolean(a.actionable))||Number(b.score||0)-Number(a.score||0));
              return next.slice(0,8);
            });
          }
        }catch{}
      };
      ws.onclose=()=>{clearInterval(ping);if(!dead)retry=setTimeout(connect,3000)};
    };
    connect();
    return()=>{dead=true;clearInterval(ping);clearTimeout(retry);try{ws?.close()}catch{}};
  },[]);

  async function connectBroker(e){
    e.preventDefault();
    if(!/^\d{6}$/.test(totp)){setLoginError("Enter current 6-digit TOTP");return;}
    setBusy(true);setLoginError("");setMessage("Connecting...");
    try{
      const r=await fetch(API+"/api/kotak/connect",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({totp})});
      const d=await r.json();
      if(!r.ok) throw new Error(typeof d?.detail==="string"?d.detail:d?.detail?.message||"Connection failed");
      setTotp("");setMessage("Live feed connected.");
    }catch(e){setTotp("");setLoginError(e?.message||"Connection failed");}
    finally{setBusy(false);}
  }

  async function scannerAction(path,key){
    setScannerBusy(key);
    try{
      const r=await fetch(API+path,{method:"POST"});
      const d=await r.json();
      setMessage(d?.message||"Scanner updated.");
      await loadScanners(); await loadStocks();
    }catch(e){setMessage(`Scanner error: ${e?.message||e}`)}
    finally{setScannerBusy("")}
  }

  function openManualOrder({kind,signal}){
    if(!signal?.actionable){setMessage("Only actionable signals can open the order dialog.");return;}
    if(kind==="INDEX_OPTION"){
      const c=signal.option_contract||{};
      const symbol=c.trading_symbol||c.display_symbol;
      if(!symbol){setMessage("Option contract is not ready.");return;}
      setOrderDraft({exchange_segment:c.exchange_segment||"nse_fo",trading_symbol:symbol,side:"B",product:"MIS",quantity:Number(c.lot_size||1)});
      return;
    }
    setOrderDraft({exchange_segment:"nse_cm",trading_symbol:`${signal.symbol}-EQ`,side:signal.direction==="SELL"?"S":"B",product:"MIS",quantity:1});
  }

  async function submitManualOrder(draft){
    setOrderBusy(true);
    try{
      const r=await fetch(API+"/api/orders/manual",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({
        exchange_segment:draft.exchange_segment,trading_symbol:draft.trading_symbol,transaction_type:draft.side,
        quantity:draft.quantity,product:draft.product,order_type:draft.order_type,price:draft.price||0,validity:"DAY",
        confirm:true,client_request_id:`WEB-${Date.now()}`
      })});
      const d=await r.json();
      if(!r.ok) throw new Error(typeof d?.detail==="string"?d.detail:JSON.stringify(d?.detail||d));
      setOrderDraft(null);setMessage("Manual order submitted.");setTimeout(loadPositions,1200);
    }catch(e){setMessage(`Order failed: ${e?.message||e}`)}
    finally{setOrderBusy(false);}
  }

  async function squareOff(p){
    const qty=Math.abs(Number(p.net_quantity));
    if(!window.confirm(`Square off ${qty} ${p.trading_symbol}?\n\nThis is a REAL market order.`)) return;
    try{
      const r=await fetch(API+"/api/positions/square-off",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({
        exchange_segment:p.exchange_segment,trading_symbol:p.trading_symbol,quantity:qty,current_net_quantity:Number(p.net_quantity),
        product:p.product||"MIS",confirm:true
      })});
      const d=await r.json();
      if(!r.ok) throw new Error(typeof d?.detail==="string"?d.detail:JSON.stringify(d?.detail||d));
      setMessage(`Square-off submitted: ${p.trading_symbol}`);setTimeout(loadPositions,1200);
    }catch(e){setMessage(`Square-off failed: ${e?.message||e}`)}
  }

  async function squareOffAll(){
    if(!positions.length) return;
    if(!window.confirm(`Square off ALL ${positions.length} positions?`)) return;
    const typed=window.prompt('Type "SQUARE OFF ALL" to confirm.');
    if(typed!=="SQUARE OFF ALL") return;
    try{
      const r=await fetch(API+"/api/positions/square-off-all",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({confirm_text:typed})});
      const d=await r.json();
      if(!r.ok) throw new Error(typeof d?.detail==="string"?d.detail:JSON.stringify(d?.detail||d));
      setMessage(`Square Off All submitted for ${d.count||0} positions.`);setTimeout(loadPositions,1500);
    }catch(e){setMessage(`Square Off All failed: ${e?.message||e}`)}
  }

  const showLogin=!status.feed_connected||!!loginError;
  const niftySignal=signals["NIFTY 50"];

  return (
    <div className="terminal-shell">
      <header className="topbar">
        <div className="brand">
          <Crown size={34}/>
          <div><h1>KING BRO</h1><b>SCALP TERMINAL</b><small>TRADE SMART • TRADE FAST</small></div>
        </div>

        <nav className="top-nav">
          <button className="active"><Home size={16}/>Dashboard</button>
          <button><Crosshair size={16}/>Index Scan</button>
          <button><ScanLine size={16}/>Stock Scan</button>
          <button><Zap size={16}/>Signals</button>
          <button><BriefcaseBusiness size={16}/>Positions</button>
          <button><Settings size={16}/>Settings</button>
        </nav>

        <div className="top-live">
          <span className={status.feed_connected?"live":"offline"}>
            {status.feed_connected?<Wifi size={15}/>:<WifiOff size={15}/>}
            {status.feed_connected?"LIVE":"OFFLINE"}
          </span>
        </div>
      </header>

      <aside className="side-nav">
        {[["Home",Home],["Index Scan",Crosshair],["Stock Scan",ScanLine],["Signals",BellRing],["Positions",BriefcaseBusiness],["Settings",Settings]].map(([label,Icon],i)=>(
          <button className={i===0?"active":""} key={label}><Icon size={23}/><span>{label}</span></button>
        ))}
        <div className="side-status"><Radio size={15}/><span>{status.feed_connected?"Connected":"Disconnected"}</span></div>
      </aside>

      <main className="main-stage">
        <section className="top-grid">
          <div className="market-overview glass-card">
            <div className="box-title">MARKET OVERVIEW</div>
            <div className="market-pair">
              <MarketMini item={feed["NIFTY 50"]}/>
              <MarketMini item={feed["SENSEX"]}/>
            </div>
          </div>

          <ScannerControl scanners={scanners} busy={scannerBusy} action={scannerAction}/>

          <SentimentGauge signals={signals}/>

          <div className="feed-card glass-card">
            <div className="box-title">LIVE FEED</div>
            <div className="feed-value">{status.feed_connected?"CONNECTED":"OFFLINE"}</div>
            <small>{status.last_tick_at ? new Date(status.last_tick_at).toLocaleTimeString("en-IN") : "Waiting for tick"}</small>
            {showLogin && (
              <form className="otp-inline" onSubmit={connectBroker}>
                <input value={totp} maxLength={6} inputMode="numeric" placeholder="TOTP"
                  onChange={e=>setTotp(e.target.value.replace(/\D/g,"").slice(0,6))}/>
                <button disabled={busy||totp.length!==6}>{busy?"...":"CONNECT"}</button>
              </form>
            )}
            {loginError && <div className="tiny-error">{loginError}</div>}
          </div>
        </section>

        <section className="middle-grid">
          <div className="chart-card glass-card">
            <div className="chart-head">
              <div><h2>NIFTY 50 · 1m</h2><div className="chart-tabs"><b>1m</b><span>5m</span><span>15m</span><span>1H</span><span>D</span></div></div>
              <BarChart3 size={20}/>
            </div>
            <CandleChart candles={niftySnapshot?.one_minute_candles || []} latestLtp={feed["NIFTY 50"]?.ltp}/>
          </div>

          <KeyLevels snapshot={niftySnapshot}/>
          <ScalpingSignal signal={niftySignal} onOrder={openManualOrder}/>
        </section>

        <IndicatorStrip snapshot={niftySnapshot}/>

        <section className="bottom-grid">
          <StockTable items={stockSignals} onOrder={openManualOrder}/>
          <RecentSignals history={history}/>
          <Positions positions={positions} loading={positionsLoading} refresh={loadPositions}
            squareOff={squareOff} squareOffAll={squareOffAll}/>
        </section>

        {message && <div className="status-toast">{message}</div>}
      </main>

      <ManualOrderModal draft={orderDraft} busy={orderBusy} close={()=>setOrderDraft(null)} submit={submitManualOrder}/>
    </div>
  );
}

createRoot(document.getElementById("root")).render(<App />);
