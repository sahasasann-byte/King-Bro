
import React from "react";
import {createRoot} from "react-dom/client";
import {Crown, LayoutDashboard, Activity, BarChart3, Wifi, WifiOff, RefreshCw} from "lucide-react";
import "./styles.css";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";
const INITIAL = [
  {name:"NIFTY 50", exchange_segment:"nse_cm"},
  {name:"SENSEX", exchange_segment:"bse_cm"},
  {name:"BANK NIFTY", exchange_segment:"nse_cm"},
];

function fmt(v){
  if(v === null || v === undefined || Number.isNaN(Number(v))) return "—";
  return Number(v).toLocaleString("en-IN",{minimumFractionDigits:2,maximumFractionDigits:2});
}

function Spark({values, negative}){
  if(values.length < 2) return <div className="spark-empty">Collecting real history…</div>;
  const w=260,h=78,p=4;
  const min=Math.min(...values), max=Math.max(...values);
  const range=(max-min)||1;
  const pts=values.map((v,i)=>{
    const x=p+(i/(values.length-1))*(w-p*2);
    const y=h-p-((v-min)/range)*(h-p*2);
    return `${x},${y}`;
  }).join(" ");
  return <svg viewBox={`0 0 ${w} ${h}`} className="spark">
    <polyline points={pts} fill="none" stroke={negative?"#ff5c79":"#37eda0"} strokeWidth="2.5"/>
  </svg>;
}

function Card({x, history}){
  const negative = x.percent_change != null && Number(x.percent_change) < 0;
  return <div className="card glass">
    <div className="card-head"><div><h3>{x.name}</h3><small>{x.name==="SENSEX"?"BSE":"NSE"}</small></div><Activity className={negative?"neg":"pos"}/></div>
    <div className="price">{fmt(x.ltp)}</div>
    <div className={negative?"change neg":"change pos"}>
      {x.change == null ? "Waiting for Kotak quote" :
        `${negative?"▼":"▲"} ${fmt(Math.abs(x.change))} (${negative?"":"+"}${fmt(x.percent_change)}%)`
      }
    </div>
    <Spark values={history} negative={negative}/>
    <div className="hl"><span>HIGH <b>{fmt(x.high)}</b></span><span>LOW <b>{fmt(x.low)}</b></span></div>
  </div>
}

function App(){
  const [indices,setIndices]=React.useState(INITIAL);
  const [status,setStatus]=React.useState("connecting");
  const [error,setError]=React.useState("");
  const [updated,setUpdated]=React.useState(null);
  const [history,setHistory]=React.useState({"NIFTY 50":[],"SENSEX":[],"BANK NIFTY":[]});

  async function load(){
    try{
      const r=await fetch(API+"/api/market/quotes",{cache:"no-store"});
      const d=await r.json();
      if(!r.ok){
        const msg = typeof d.detail === "object" ? (d.detail.message || JSON.stringify(d.detail)) : d.detail;
        throw new Error(msg || "Kotak quotes request failed");
      }
      setIndices(d.indices);
      setHistory(prev=>{
        const next={...prev};
        d.indices.forEach(x=>{
          if(typeof x.ltp === "number"){
            next[x.name]=[...(prev[x.name]||[]),x.ltp].slice(-60);
          }
        });
        return next;
      });
      setUpdated(new Date());
      setStatus("live");
      setError("");
    }catch(e){
      setStatus("offline");
      setError(e.message || String(e));
    }
  }

  React.useEffect(()=>{
    load();
    const id=setInterval(load,3000);
    return()=>clearInterval(id);
  },[]);

  return <div className="shell">
    <aside className="sidebar glass">
      <div className="brand"><Crown/><h2>KING</h2><h2 className="bro">BRO</h2><span>TERMINAL</span></div>
      <nav>
        <div className="nav active"><LayoutDashboard/>Dashboard</div>
        <div className="nav"><Activity/>Live Quotes</div>
        <div className="nav"><BarChart3/>Charts</div>
      </nav>
      <div className="conn glass">
        {status==="live"?<Wifi/>:<WifiOff/>}
        <div><b>{status==="live"?"CONNECTED":"DISCONNECTED"}</b><small>Kotak Neo Quotes API</small><small>No mock data</small></div>
      </div>
    </aside>

    <main>
      <header>
        <div><h1>King Bro <span>Terminal</span></h1><p>Real Market. Real Data. No Mock Prices.</p></div>
        <div className={status==="live"?"pill live":"pill off"}>● {status==="live"?"LIVE":"OFFLINE"}</div>
      </header>

      <section className="cards">
        {indices.map(x=><Card key={x.name} x={x} history={history[x.name]||[]}/>)}
      </section>

      <section className="overview glass">
        <div className="overview-head">
          <div><h3>Live Market Overview</h3><small>{status==="live"?"● Real Kotak quote polling":"● Disconnected"}</small></div>
          <button onClick={load}><RefreshCw/> Refresh</button>
        </div>
        <div className="status-grid">
          <div><span>DATA SOURCE</span><b>Kotak Neo</b><small>Quotes API</small></div>
          <div><span>AUTH</span><b>Consumer Key</b><small>No TOTP / MPIN</small></div>
          <div><span>REFRESH</span><b>3 Seconds</b><small>Single 3-index request</small></div>
          <div><span>LAST UPDATE</span><b>{updated?updated.toLocaleTimeString("en-IN"):"—"}</b><small>Browser time</small></div>
        </div>
        {error && <div className="error"><b>Kotak API error:</b> {error}</div>}
        {!error && <div className="tip">Live values and mini-charts are built only from Kotak quote responses received by this browser session.</div>}
      </section>
    </main>
  </div>
}

createRoot(document.getElementById("root")).render(<App/>);
