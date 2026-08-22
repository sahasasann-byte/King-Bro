
import React from "react";
import {createRoot} from "react-dom/client";
import {Crown,Wifi,WifiOff} from "lucide-react";
import "./styles.css";
const API=import.meta.env.VITE_API_URL||"http://localhost:8000";
const WS=API.replace(/^http/,"ws")+"/ws/market";
const base={"NIFTY 50":{key:"NIFTY 50",ltp:null},"SENSEX":{key:"SENSEX",ltp:null},"BANK NIFTY":{key:"BANK NIFTY",ltp:null}};
const fmt=v=>v==null?"—":Number(v).toLocaleString("en-IN",{minimumFractionDigits:2,maximumFractionDigits:2});
function App(){
 const [ticks,setTicks]=React.useState(base),[status,setStatus]=React.useState({broker_connected:false,feed_connected:false,last_error:null}),[totp,setTotp]=React.useState(""),[msg,setMsg]=React.useState("");
 React.useEffect(()=>{let ws,t,p;const open=()=>{ws=new WebSocket(WS);ws.onopen=()=>{ws.send("hi");p=setInterval(()=>ws.readyState===1&&ws.send("ping"),20000)};ws.onmessage=e=>{const m=JSON.parse(e.data);if(m.type==="status")setStatus(m.data);if(m.type==="snapshot"){const n={...base};m.data.forEach(x=>n[x.key]&&(n[x.key]=x));setTicks(n)}if(m.type==="tick")setTicks(v=>({...v,[m.data.key]:m.data}))};ws.onclose=()=>{clearInterval(p);t=setTimeout(open,3000)}};open();return()=>{clearTimeout(t);clearInterval(p);ws?.close()}},[]);
 async function login(e){e.preventDefault();if(!/^\d{6}$/.test(totp)){setMsg("Enter current 6-digit TOTP");return}setMsg("Connecting...");const r=await fetch(API+"/api/kotak/connect",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({totp})});const d=await r.json();setMsg(r.ok?d.message:(d.detail||"Login failed"));if(r.ok)setTotp("")}
 return <div className="page"><aside className="side glass"><Crown/><h2>KING <span>BRO</span></h2><small>TERMINAL</small><div className="conn">{status.feed_connected?<Wifi/>:<WifiOff/>}<b>{status.feed_connected?"CONNECTED":"DISCONNECTED"}</b></div></aside><main><header><h1>King Bro Terminal</h1><span className={status.feed_connected?"live":"off"}>● {status.feed_connected?"LIVE":"OFFLINE"}</span></header>{!status.broker_connected&&<form className="login glass" onSubmit={login}><div><h3>Morning Kotak Login</h3><p>Consumer Key, Mobile, UCC and MPIN are stored in Render.</p></div><input value={totp} onChange={e=>setTotp(e.target.value.replace(/\D/g,"").slice(0,6))} placeholder="6-digit TOTP" inputMode="numeric"/><button>CONNECT LIVE DATA</button></form>}<section className="cards">{Object.values(ticks).map(x=><div className="card glass" key={x.key}><h3>{x.key}</h3><b>{fmt(x.ltp)}</b><small>{x.received_at?"LIVE "+new Date(x.received_at).toLocaleTimeString():"Waiting for Kotak feed"}</small></div>)}</section><section className="monitor glass"><h3>Live Market Monitor</h3><p>{status.feed_connected?"Real-time Kotak ticks are arriving.":"Enter TOTP to start the authenticated Kotak session."}</p></section><footer>{msg}{status.last_error?" • "+status.last_error:""}</footer></main></div>
}
createRoot(document.getElementById("root")).render(<App/>);
