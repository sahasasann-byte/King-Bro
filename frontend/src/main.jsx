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
    <div className={`mini-market glass-card ${item?.received_at ? "tick-flash" : ""}`}>
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

function ScannerControl({ scanners, busy, action, engineAction }) {
  const indexEnabled = Boolean(scanners?.index?.enabled);
  const stockEnabled = Boolean(scanners?.stocks?.enabled);
  const stockRunning = Boolean(scanners?.stocks?.running);
  const engineEnabled = indexEnabled && stockEnabled;

  const resolved = Number(scanners?.stocks?.resolved || 0);
  const unresolved = Number(scanners?.stocks?.unresolved || 0);
  const stockError = scanners?.stocks?.last_error || "";

  const Row = ({
    label,
    sub,
    enabled,
    live,
    startPath,
    stopPath,
    keyName,
    stateText,
  }) => (
    <div className="scan-row">
      <div>
        <b>{label}</b>
        <small>{sub}</small>
        <small className={live ? "pos" : enabled ? "" : "neg"}>
          {stateText}
        </small>
      </div>

      <button
        type="button"
        className={`toggle-visual toggle-button ${enabled ? "on" : ""}`}
        disabled={busy === `${keyName}-start` || busy === `${keyName}-stop`}
        aria-label={`${label} ${enabled ? "stop" : "start"}`}
        onClick={() =>
          action(
            enabled ? stopPath : startPath,
            `${keyName}-${enabled ? "stop" : "start"}`
          )
        }
      >
        <i />
      </button>

      <button
        type="button"
        className="scan-start"
        disabled={busy === `${keyName}-start` || enabled}
        onClick={(e) => { e.preventDefault(); e.stopPropagation(); action(startPath, `${keyName}-start`); }}
      >
        <Play size={13}/>
        {busy === `${keyName}-start` ? "STARTING…" : "START"}
      </button>

      <button
        type="button"
        className="scan-stop"
        disabled={busy === `${keyName}-stop` || !enabled}
        onClick={(e) => { e.preventDefault(); e.stopPropagation(); action(stopPath, `${keyName}-stop`); }}
      >
        <Square size={12}/>
        {busy === `${keyName}-stop` ? "STOPPING…" : "STOP"}
      </button>
    </div>
  );

  return (
    <div className="scan-control glass-card">
      <div className="scan-headline">
        <div className="box-title">SCAN CONTROLS</div>

        <div className="engine-master">
          <span className={engineEnabled ? "engine-on" : "engine-off"}>
            {engineEnabled ? "ENGINE ON" : "ENGINE OFF"}
          </span>

          <button
            type="button"
            className="engine-start"
            disabled={Boolean(busy) || engineEnabled}
            onClick={() => engineAction("start")}
          >
            <Play size={12}/> ENGINE START
          </button>

          <button
            type="button"
            className="engine-stop"
            disabled={Boolean(busy) || (!indexEnabled && !stockEnabled)}
            onClick={() => engineAction("stop")}
          >
            <Square size={11}/> ENGINE STOP
          </button>
        </div>
      </div>

      <Row
        label="NIFTY + SENSEX"
        sub="Index signal engine"
        enabled={indexEnabled}
        live={indexEnabled}
        stateText={indexEnabled ? "● RUNNING" : "● STOPPED"}
        startPath="/api/scanners/index/start"
        stopPath="/api/scanners/index/stop"
        keyName="index"
      />

      <Row
        label="STOCK SCAN"
        sub="Fixed 40-stock universe"
        enabled={stockEnabled}
        live={stockRunning}
        stateText={
          stockRunning
            ? `● LIVE · ${resolved}/40`
            : stockError
              ? "● ERROR"
              : stockEnabled
                ? "● STARTING"
                : "● STOPPED"
        }
        startPath="/api/scanners/stocks/start"
        stopPath="/api/scanners/stocks/stop"
        keyName="stock"
      />

      <div className="scan-stats">
        <span>Universe <b>{scanners?.stocks?.universe_count ?? 40}</b></span>
        <span>Resolved <b>{resolved}</b></span>
        <span>Unresolved <b>{unresolved}</b></span>
        <span>
          Status{" "}
          <b className={stockRunning ? "pos" : stockError ? "neg" : ""}>
            {stockRunning
              ? "LIVE"
              : stockError
                ? "ERROR"
                : stockEnabled
                  ? "STARTING"
                  : "STOP"}
          </b>
        </span>
      </div>

      {stockError && <div className="scanner-inline-error">{stockError}</div>}
    </div>
  );
}

function SentimentGauge({ signals }) {
  const rows = ["NIFTY 50", "SENSEX"].map((symbol) => {
    const s = signals?.[symbol];
    if (!s) return null;

    const tech = s?.technical_scores || {};
    let bull = Number(tech?.bullish);
    let bear = Number(tech?.bearish);

    // Backward-compatible fallback for one deploy cycle while old signal
    // payloads may still be cached in the browser.
    if (!Number.isFinite(bull) || !Number.isFinite(bear)) {
      const score = Number(s?.score || 0);
      bull = s?.direction === "CALL" ? score : 0;
      bear = s?.direction === "PUT" ? score : 0;
    }

    if (!Number.isFinite(bull) || !Number.isFinite(bear)) return null;
    const net = Math.max(-100, Math.min(100, bull - bear));
    return { symbol, bull, bear, net, direction:s?.direction, score:Number(s?.score || 0) };
  }).filter(Boolean);

  if (!rows.length) {
    return (
      <div className="sentiment glass-card mobile-compact-card">
        <div className="box-title">MARKET SENTIMENT</div>
        <div className="gauge" style={{"--sentiment":"50%", "--sentiment-color":"#31d9ff"}}>
          <div className="gauge-inner"><span>WARMING UP</span><b>50</b><small>/100</small></div>
        </div>
        <div className="sentiment-source">Waiting for technical snapshot</div>
      </div>
    );
  }

  const avgNet = rows.reduce((sum, r) => sum + r.net, 0) / rows.length;
  const bias = Math.max(0, Math.min(100, Math.round(50 + avgNet / 2)));

  let label = "NEUTRAL";
  let tone = "#31d9ff";
  if (bias >= 70) { label = "STRONG BULLISH"; tone = "#26f59a"; }
  else if (bias >= 58) { label = "BULLISH"; tone = "#26f59a"; }
  else if (bias <= 30) { label = "STRONG BEARISH"; tone = "#ff6078"; }
  else if (bias <= 42) { label = "BEARISH"; tone = "#ff6078"; }

  return (
    <div className="sentiment glass-card mobile-compact-card">
      <div className="box-title">MARKET SENTIMENT</div>
      <div className="gauge" style={{"--sentiment":`${bias}%`, "--sentiment-color":tone}}>
        <div className="gauge-inner">
          <span style={{color:tone}}>{label}</span>
          <b>{bias}</b>
          <small>/100</small>
        </div>
      </div>
      <div className="sentiment-mini">
        {rows.map((r) => (
          <span key={r.symbol}>
            <b>{r.symbol === "NIFTY 50" ? "NIFTY" : "SENSEX"}</b>
            <em className={r.net > 8 ? "pos" : r.net < -8 ? "neg" : ""}>
              {r.net > 8 ? "BULL" : r.net < -8 ? "BEAR" : "NEUTRAL"} {Math.round(50 + r.net/2)}
            </em>
          </span>
        ))}
      </div>
      <div className="sentiment-source">TECHNICAL BIAS • option filter does not alter this reading</div>
    </div>
  );
}

function KeyLevels({ snapshot }) {
  const [mode, setMode] = React.useState("daily");
  const indicators = snapshot?.indicators || {};
  const levels =
    mode === "daily"
      ? (indicators?.daily_levels || {})
      : (indicators?.five_minute_levels || {});

  const dailyReady = Boolean(indicators?.daily_levels?.ready);
  const fiveReady = Boolean(indicators?.five_minute_levels?.ready);
  const fib = levels?.fib || {};
  const ready = Boolean(levels?.ready);

  React.useEffect(() => {
    if (mode === "daily" && !dailyReady && fiveReady) setMode("five");
    if (mode === "five" && !fiveReady && dailyReady) setMode("daily");
  }, [mode, dailyReady, fiveReady]);

  if (!dailyReady && !fiveReady) return null;

  return (
    <div className="key-levels glass-card">
      <div className="box-title">KEY LEVELS</div>

      <div className="tabs-lite">
        <button type="button" disabled={!dailyReady} className={mode==="daily" ? "active" : ""} onClick={()=>setMode("daily")}>Daily</button>
        <button type="button" disabled={!fiveReady} className={mode==="five" ? "active" : ""} onClick={()=>setMode("five")}>5 Min</button>
      </div>

      <div className="levels-list">
        <span><em>R2</em><b>{ready ? n(fib?.r2) : "—"}</b></span>
        <span><em>R1</em><b>{ready ? n(fib?.r1) : "—"}</b></span>
        <span><em>P</em><b>{ready ? n(levels?.pivot) : "—"}</b></span>
        <span><em>S1</em><b>{ready ? n(fib?.s1) : "—"}</b></span>
        <span><em>S2</em><b>{ready ? n(fib?.s2) : "—"}</b></span>
      </div>

      {!ready && (
        <div className="levels-wait">
          {levels?.reason || (mode==="daily"
            ? "Waiting for completed previous-session candles."
            : "Waiting for completed 5-minute candle.")}
        </div>
      )}
    </div>
  );
}

function signalTimestampMs(signal) {
  const raw =
    signal?.generated_at ||
    signal?.created_at ||
    signal?.timestamp ||
    signal?.signal_time ||
    signal?.updated_at;

  if (!raw) return null;
  const ms = Date.parse(raw);
  return Number.isFinite(ms) ? ms : null;
}

function signalAgeState(signal, nowMs) {
  const ts = signalTimestampMs(signal);
  if (!ts) return { stale:false, ageText:"LIVE" };

  const ageMs = Math.max(0, nowMs - ts);
  const mins = Math.floor(ageMs / 60000);

  return {
    stale: ageMs >= 15 * 60000,
    ageText: mins < 1 ? "NOW" : `${mins}m`,
  };
}

function approxRisk(signal) {
  const entry = Number(signal?.entry);
  const sl = Number(signal?.stop_loss);
  const qty = Number(
    signal?.quantity ||
    signal?.qty ||
    signal?.lot_size ||
    1
  );

  if (![entry, sl, qty].every(Number.isFinite)) return null;
  return Math.abs(entry - sl) * Math.max(1, qty);
}

function ScalpingSignal({
  indexSignals,
  stockSignals,
  onOrder,
  nowMs
}) {
  const combined = [];

  for (const key of ["NIFTY 50", "SENSEX"]) {
    const signal = indexSignals?.[key];
    if (!signal) continue;

    combined.push({
      kind: "INDEX_OPTION",
      signal,
      symbol: key,
      direction: signal.direction,
      grade: signal.grade,
      score: signal.score,
      actionable: Boolean(signal.actionable),
      orderReady: Boolean(
        signal.actionable &&
        signal.option_contract
      ),
      optionName:
        signal.option_contract?.display_symbol ||
        signal.option_contract?.trading_symbol ||
        "",
      entry: signal.entry,
      stop_loss: signal.stop_loss,
      target_1: signal.target_1,
      target_2: signal.target_2,
      reasons: signal.reasons || [],
      blockers: signal.blockers || [],
      ...signalAgeState(signal, nowMs),
      approxRisk: approxRisk(signal),
    });
  }

  for (const signal of stockSignals || []) {
    const grade = String(signal?.grade || "").toUpperCase();
    const score = Number(signal?.score || 0);

    if (
      grade === "WARMING_UP" ||
      grade === "NO_TRADE" ||
      grade === "ATR_NOT_READY" ||
      (!signal?.actionable && score < 60)
    ) continue;

    combined.push({
      kind: "STOCK",
      signal,
      symbol: signal.symbol,
      direction: signal.direction,
      grade: signal.grade,
      score: signal.score,
      actionable: Boolean(signal.actionable),
      orderReady: Boolean(signal.actionable),
      optionName: "",
      entry: signal.entry,
      stop_loss: signal.stop_loss,
      target_1: signal.target_1,
      target_2: signal.target_2,
      reasons: signal.reasons || [],
      ...signalAgeState(signal, nowMs),
      approxRisk: approxRisk(signal),
    });
  }

  combined.sort((a, b) =>
    Number(Boolean(a.stale)) -
      Number(Boolean(b.stale)) ||
    Number(Boolean(b.actionable)) -
      Number(Boolean(a.actionable)) ||
    Number(b.score || 0) -
      Number(a.score || 0)
  );

  const top = combined[0] || null;
  const rest = combined.slice(1, 7);

  const topPositive =
    top?.direction === "CALL" ||
    top?.direction === "BUY";

  const topNegative =
    top?.direction === "PUT" ||
    top?.direction === "SELL";

  return (
    <div
      className={`scalp-panel glass-card mobile-priority ${
        topPositive
          ? "call"
          : topNegative
            ? "put"
            : "wait"
      }`}
    >
      <div className="scalp-top">
        <span>
          <Zap size={18}/>
          SCALPING SIGNALS
        </span>

        <b className={top?.stale ? "stale-badge" : ""}>
          {top?.stale
            ? `STALE • ${top.ageText}`
            : top
              ? `${top.grade || "SETUP"} • ${top.ageText}`
              : "SCANNING"}
        </b>
      </div>

      {top ? (
        <>
          <div className="scalp-symbol">
            {top.symbol}
          </div>

          <div className="scalp-direction">
            <strong
              className={
                topPositive
                  ? "pos"
                  : topNegative
                    ? "neg"
                    : ""
              }
            >
              {top.direction || "WAIT"}
            </strong>

            <span>
              SCORE{" "}
              <b>{top.score ?? "—"}</b>
            </span>
          </div>

          {top.optionName && (
            <div className="top-contract">
              {top.optionName}
            </div>
          )}

          <div className="scalp-grid">
            <span>
              Entry
              <b>
                {top.entry == null
                  ? "—"
                  : `₹${n(top.entry)}`}
              </b>
            </span>

            <span>
              Stop Loss
              <b className="neg">
                {top.stop_loss == null
                  ? "—"
                  : `₹${n(top.stop_loss)}`}
              </b>
            </span>

            <span>
              Target 1
              <b className="pos">
                {top.target_1 == null
                  ? "—"
                  : `₹${n(top.target_1)}`}
              </b>
            </span>

            <span>
              Target 2
              <b className="pos">
                {top.target_2 == null
                  ? "—"
                  : `₹${n(top.target_2)}`}
              </b>
            </span>
          </div>

          <div className="risk-preview">
            <span>APPROX RISK</span>
            <b>
              {top.approxRisk == null
                ? "Qty required"
                : `₹${n(top.approxRisk)}`}
            </b>
            <small>
              Entry → Stop Loss
            </small>
          </div>

          {top.blockers?.length > 0 && (
            <div className="signal-blocker">
              <b>{Number(top.score || 0) >= 70 ? "TECHNICAL SETUP READY" : "WAITING"}</b>
              <span>{top.blockers[0]}</span>
            </div>
          )}

          <div className="confirm-box">
            <div className="confirm-title">
              CONFIRMATION
            </div>

            <div className="confirm-grid">
              {top.reasons
                .slice(0, 6)
                .map((r, i) => (
                  <span key={i}>
                    ✓ {r}
                  </span>
                ))}

              {!top.reasons.length && (
                <span>
                  Waiting for confirmations…
                </span>
              )}
            </div>
          </div>

          <button
            className="strong-action"
            disabled={!top.orderReady || top.stale}
            onClick={() =>
              onOrder({
                kind: top.kind,
                signal: top.signal,
              })
            }
          >
            {top.stale
              ? "STALE SIGNAL"
              : top.orderReady
                ? "MANUAL ORDER"
                : top.actionable
                  ? "SIGNAL • OPTION DATA PENDING"
                  : "WAITING"}
            <ChevronRight size={18}/>
          </button>
        </>
      ) : (
        <div className="combined-empty">
          Waiting for index / stock signals…
        </div>
      )}

      <div className="ranked-signals">
        <div className="ranked-title">
          NEXT BEST SIGNALS
        </div>

        {rest.map((item, i) => {
          const positive =
            item.direction === "CALL" ||
            item.direction === "BUY";

          return (
            <div
              className="ranked-row"
              key={`${item.symbol}-${i}`}
            >
              <span className="rank-no">
                {i + 2}
              </span>

              <div className="rank-symbol">
                <b>{item.symbol}</b>
                <small>
                  {item.stale
                  ? `STALE • ${item.ageText}`
                  : `${item.grade || "—"} • ${item.ageText}`}
                </small>
              </div>

              <strong
                className={
                  positive
                    ? "pos"
                    : "neg"
                }
              >
                {item.direction || "WAIT"}
              </strong>

              <span className="rank-score">
                {item.score ?? "—"}
              </span>

              <button
                type="button"
                disabled={!item.orderReady || item.stale}
                onClick={() =>
                  onOrder({
                    kind: item.kind,
                    signal: item.signal,
                  })
                }
              >
                ORDER
              </button>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function IndicatorStrip({ snapshot }) {
  const one = snapshot?.indicators?.one_minute || {};
  const five = snapshot?.indicators?.five_minute || {};
  const fifteen = snapshot?.indicators?.fifteen_minute || {};
  const blocks = [
    ["1M INDICATORS", [
      ["RSI (14)", one.rsi14], ["Williams %R", one.williams_r14],
      ["EMA 9", one.ema9], ["EMA 21", one.ema21],
    ]],
    ["5M TREND", [
      ["EMA 9", five.ema9], ["EMA 21", five.ema21],
      ["MA 20", five.ma20], ["RSI", five.rsi14], ["Action", five.price_action],
    ]],
    ["15M TREND", [
      ["EMA 9", fifteen.ema9], ["EMA 21", fifteen.ema21],
      ["RSI", fifteen.rsi14], ["Action", fifteen.price_action], ["Breakout", fifteen.breakout],
    ]],
    ["PRICE ACTION", [
      ["1M", one.price_action], ["5M", five.price_action],
      ["15M", fifteen.price_action], ["ATR", one.atr14],
    ]],
  ].map(([title, rows]) => [title, rows.filter(([,v]) => v !== null && v !== undefined && v !== "" && v !== "UNKNOWN")])
   .filter(([,rows]) => rows.length > 0);

  if (!blocks.length) return null;

  return (
    <div className="indicator-strip compact-readings">
      {blocks.map(([title, rows]) => (
        <div className="indicator-card glass-card" key={title}>
          <div className="box-title">{title}</div>
          {rows.map(([k,v]) => {
            const text = typeof v === "number" ? n(v) : String(v ?? "—");
            const bearish = /BEAR|DOWN|SELL/i.test(text);
            const bullish = /BULL|UP|BUY|BREAKOUT/i.test(text);
            return (
              <div className="indicator-row" key={k}>
                <span>{k}</span>
                <b className={bearish ? "neg" : bullish ? "pos" : ""}>{text}</b>
                <i className={bearish ? "red-dot" : "green-dot"} />
              </div>
            );
          })}
        </div>
      ))}
    </div>
  );
}

function StockTable({ items, onOrder }) {
  return (
    <div className="bottom-card glass-card recent-signals-card">
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
    <div className="bottom-card glass-card recent-history-card">
      <div className="box-title-row">
        <div className="box-title">RECENT SIGNALS / CALL HISTORY</div>
        <small>INDEX + STOCK</small>
      </div>

      <div className="recent-history-head">
        <span>TIME</span>
        <span>SYMBOL</span>
        <span>CALL</span>
        <span>SCORE</span>
        <span>ENTRY</span>
        <span>SL</span>
      </div>

      {(history || []).slice(0,8).map((s,i) => (
        <div className="recent-row recent-history-row" key={`${s?.symbol || "row"}-${i}`}>
          <span>
            {(s?.generated_at || s?.updated_at)
              ? new Date(s.generated_at || s.updated_at).toLocaleTimeString(
                  "en-IN",
                  {hour:"2-digit",minute:"2-digit"}
                )
              : "—"}
          </span>

          <b>{s?.symbol || "—"}</b>

          <span className={
            s?.direction==="CALL" || s?.direction==="BUY"
              ? "pos"
              : s?.direction==="PUT" || s?.direction==="SELL"
                ? "neg"
                : ""
          }>
            {s?.direction || "WAIT"}
          </span>

          <span>{s?.score ?? "—"}</span>
          <span>{s?.entry == null ? "—" : n(s.entry)}</span>
          <span>{s?.stop_loss == null ? "—" : n(s.stop_loss)}</span>
        </div>
      ))}

      {!(history || []).length && (
        <div className="bottom-empty">
          No qualified calls yet. Scanner history will appear here.
        </div>
      )}
    </div>
  );
}

function Positions({ positions, loading, refresh, squareOff, squareOffAll }) {
  return (
    <div className={`positions-box glass-card ${positions.length ? "has-positions" : "empty-positions"}`}>
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
  const [signalDetails,setSignalDetails] = React.useState({
    "NIFTY 50": null,
    "SENSEX": null,
  });
  const [history,setHistory] = React.useState([]);
  const [stockSignals,setStockSignals] = React.useState([]);
  const [positions,setPositions] = React.useState([]);
  const [positionsLoading,setPositionsLoading] = React.useState(false);
  const [orderDraft,setOrderDraft] = React.useState(null);
  const [orderBusy,setOrderBusy] = React.useState(false);
  const [telegramCheck,setTelegramCheck] = React.useState({state:"idle", text:"TELEGRAM CHECK"});

  const [activeSection,setActiveSection] = React.useState("dashboard");
  const [clockNow,setClockNow] = React.useState(Date.now());

  const sectionRefs = {
    dashboard: React.useRef(null),
    index: React.useRef(null),
    stocks: React.useRef(null),
    signals: React.useRef(null),
    positions: React.useRef(null),
    settings: React.useRef(null),
  };

  React.useEffect(() => {
    const id = window.setInterval(() => setClockNow(Date.now()), 30000);
    return () => window.clearInterval(id);
  }, []);

  function goTo(section) {
    setActiveSection(section);

    const target =
      section === "stocks"
        ? sectionRefs.index
        : sectionRefs[section];

    target?.current?.scrollIntoView({
      behavior: "smooth",
      block: "start",
    });
  }

  const loadScanners = React.useCallback(async()=>{
    try {
      const r = await fetch(API+"/api/scanners", { cache:"no-store" });
      if(!r.ok) return;
      const d = await r.json();
      setScanners(d);
    } catch {}
  },[]);

  const loadSignals = React.useCallback(async()=>{
    try {
      const r = await fetch(API+"/api/signals", { cache:"no-store" });
      if(!r.ok) return;
      const d = await r.json();
      const s = d?.signals || {};
      setSignals({"NIFTY 50":s["NIFTY 50"]||null,"SENSEX":s["SENSEX"]||null});
      setHistory(Array.isArray(d?.history)?d.history:[]);
    } catch {}
  },[]);


  const loadSignalDetails = React.useCallback(async()=>{
    try {
      const [niftyResponse, sensexResponse] = await Promise.all([
        fetch(API+"/api/signals/NIFTY50", { cache:"no-store" }),
        fetch(API+"/api/signals/SENSEX", { cache:"no-store" }),
      ]);

      const next = {};

      if(niftyResponse.ok){
        const d = await niftyResponse.json();
        next["NIFTY 50"] = d;
      }

      if(sensexResponse.ok){
        const d = await sensexResponse.json();
        next["SENSEX"] = d;
      }

      if(Object.keys(next).length){
        setSignalDetails(prev => ({...prev, ...next}));
      }
    } catch {}
  },[]);


  const loadStocks = React.useCallback(async()=>{
    try {
      const r = await fetch(API+"/api/stocks/signals/best?limit=8", { cache:"no-store" });
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

  // LIGHT MODE: WebSocket remains the primary live path. REST is only a
  // low-frequency safety refresh, and it pauses while the tab is hidden.
  // Signal/strategy calculations are unchanged.
  React.useEffect(()=>{
    const refreshCore = ()=>{
      if(document.visibilityState !== "visible") return;
      loadScanners();
      loadSignals();
      loadStocks();
    };

    refreshCore();
    loadSignalDetails();

    const coreTimer=setInterval(refreshCore,60000);
    const detailTimer=setInterval(()=>{
      if(document.visibilityState === "visible") loadSignalDetails();
    },120000);

    const onVisible=()=>{
      if(document.visibilityState === "visible") refreshCore();
    };
    document.addEventListener("visibilitychange",onVisible);

    return()=>{
      clearInterval(coreTimer);
      clearInterval(detailTimer);
      document.removeEventListener("visibilitychange",onVisible);
    };
  },[loadScanners,loadSignals,loadSignalDetails,loadStocks]);

  React.useEffect(()=>{
    if(!status.broker_connected) return;
    const refreshPositions=()=>{
      if(document.visibilityState === "visible") loadPositions();
    };
    refreshPositions();
    const t=setInterval(refreshPositions,60000);
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
            if(s?.symbol==="NIFTY 50"||s?.symbol==="SENSEX"){
              setSignals(p=>({...p,[s.symbol]:s}));
              if(s?.indicators){
                setSignalDetails(p=>({
                  ...p,
                  [s.symbol]: {
                    ...(p[s.symbol] || {}),
                    signal: s,
                    indicators: s.indicators,
                  },
                }));
              }
            }
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

  async function checkTelegram(){
    if(telegramCheck.state === "checking") return;
    setTelegramCheck({state:"checking", text:"CHECKING…"});
    try{
      const statusResponse = await fetch(API+"/api/telegram/status", {cache:"no-store"});
      const statusData = await statusResponse.json();
      if(!statusResponse.ok) throw new Error(statusData?.detail || "Telegram status failed");
      if(!statusData?.configured){
        setTelegramCheck({state:"error", text:"NOT CONFIGURED"});
        return;
      }

      const testResponse = await fetch(API+"/api/telegram/test", {
        method:"POST",
        cache:"no-store",
        headers:{"Accept":"application/json"}
      });
      const testData = await testResponse.json();
      if(!testResponse.ok){
        const detail = typeof testData?.detail === "string" ? testData.detail : "Telegram test failed";
        throw new Error(detail);
      }
      setTelegramCheck({state:"ok", text:"TELEGRAM OK ✓"});
      setMessage("Telegram test message sent.");
    }catch(e){
      setTelegramCheck({state:"error", text:"TELEGRAM ERROR"});
      setMessage(`Telegram: ${e?.message||e}`);
    }
  }

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

  async function refreshScannerBurst(includeStocks=false){
    await loadScanners();
    if(includeStocks) await loadStocks();

    window.setTimeout(() => {
      loadScanners();
      if(includeStocks) loadStocks();
    }, 1200);

    window.setTimeout(() => {
      loadScanners();
      if(includeStocks) loadStocks();
    }, 3500);
  }

  async function scannerAction(path,key){
    // Lock only this scanner action. A stale/hung action must never disable
    // every scanner control on the dashboard.
    if(scannerBusy === key) return;

    const isStart = key.endsWith("-start");

    if(
      key === "stock-start" &&
      scanners?.stocks?.enabled
    ){
      setMessage(
        scanners?.stocks?.running
          ? "Stock scanner is already LIVE."
          : "Stock scanner is already STARTING. Please wait."
      );
      return;
    }

    if(
      key === "index-start" &&
      scanners?.index?.enabled
    ){
      setMessage("Index scanner is already RUNNING.");
      return;
    }
    const isStock = key.startsWith("stock-");
    const isIndex = key.startsWith("index-");

    setScannerBusy(key);
    setMessage(isStart ? "Starting scanner…" : "Stopping scanner…");

    // Immediate visual reaction; authoritative state is refreshed below.
    setScanners(prev => ({
      ...prev,
      index: isIndex
        ? {...(prev?.index || {}), enabled:isStart}
        : prev?.index,
      stocks: isStock
        ? {
            ...(prev?.stocks || {}),
            enabled:isStart,
            running:isStart ? Boolean(prev?.stocks?.running) : false,
            last_error:null,
          }
        : prev?.stocks,
    }));

    try{
      const controller = new AbortController();
      const timeoutId = window.setTimeout(() => controller.abort(), 12000);
      let r;
      try {
        r=await fetch(API+path,{
          method:"POST",
          cache:"no-store",
          headers:{"Accept":"application/json"},
          signal: controller.signal,
        });
      } finally {
        window.clearTimeout(timeoutId);
      }

      const d=await r.json();

      if(!r.ok){
        const detail =
          typeof d?.detail === "string"
            ? d.detail
            : d?.detail?.message || d?.message || `Scanner request failed (${r.status})`;
        throw new Error(detail);
      }

      setMessage(
        d?.message ||
        (isStart ? "Scanner started." : "Scanner stopped.")
      );

      await refreshScannerBurst(isStock);
      if(isIndex){
        await loadSignals();
        await loadSignalDetails();
      }
    }catch(e){
      const msg = e?.name === "AbortError"
        ? "Scanner request timed out. Please try again."
        : (e?.message || e);
      setMessage(`Scanner error: ${msg}`);
      await loadScanners();
    }finally{
      setScannerBusy("");
    }
  }

  async function engineAction(mode){
    if(scannerBusy) return;

    const start = mode === "start";
    setScannerBusy(`engine-${mode}`);
    setMessage(start ? "Starting INDEX + STOCK engine…" : "Stopping INDEX + STOCK engine…");

    try{
      const paths = start
        ? ["/api/scanners/index/start", "/api/scanners/stocks/start"]
        : ["/api/scanners/stocks/stop", "/api/scanners/index/stop"];

      for(const path of paths){
        const r = await fetch(API+path,{
          method:"POST",
          cache:"no-store",
          headers:{"Accept":"application/json"}
        });
        const d = await r.json();
        if(!r.ok){
          const detail =
            typeof d?.detail === "string"
              ? d.detail
              : d?.detail?.message || d?.message || `Engine request failed (${r.status})`;
          throw new Error(detail);
        }
      }

      setMessage(start ? "INDEX + STOCK engine started." : "INDEX + STOCK engine stopped.");
      await refreshScannerBurst(true);
      await loadSignals();
      await loadSignalDetails();
    }catch(e){
      setMessage(`Engine error: ${e?.message||e}`);
      await loadScanners();
    }finally{
      setScannerBusy("");
    }
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

  const showLogin=!status.broker_connected||!!loginError;
  const niftySignal = signals["NIFTY 50"];
  const niftyDisplaySnapshot =
    niftySignal ||
    (signalDetails["NIFTY 50"]?.indicators
      ? { indicators: signalDetails["NIFTY 50"].indicators }
      : null);
  const hasKeyLevels = Boolean(
    niftyDisplaySnapshot?.indicators?.daily_levels?.ready ||
    niftyDisplaySnapshot?.indicators?.five_minute_levels?.ready
  );

  const combinedHistory = React.useMemo(() => {
    const rows = [
      ...(Array.isArray(history) ? history : []),
      ...(Array.isArray(stockSignals) ? stockSignals : []),
    ];

    const seen = new Set();

    return rows
      .filter(Boolean)
      .sort((a,b) => {
        const ta = Date.parse(a?.generated_at || a?.updated_at || 0) || 0;
        const tb = Date.parse(b?.generated_at || b?.updated_at || 0) || 0;
        return tb - ta;
      })
      .filter((row) => {
        const key = `${row?.symbol || ""}|${row?.direction || ""}|${row?.generated_at || row?.updated_at || ""}`;
        if(seen.has(key)) return false;
        seen.add(key);
        return true;
      })
      .slice(0,20);
  }, [history, stockSignals]);

  return (
    <div className="terminal-shell">
      <header className="topbar">
        <div className="brand">
          <Crown size={34}/>
          <div><h1>KING BRO</h1><b>SCALP TERMINAL</b><small>TRADE SMART • TRADE FAST</small></div>
        </div>

        <div className="top-live">
          <span className={status.feed_connected?"live":"offline"}>
            {status.feed_connected?<Wifi size={15}/>:<WifiOff size={15}/>}
            {status.feed_connected?"LIVE":"OFFLINE"}
          </span>
        </div>
      </header>

      <aside className="side-nav">
        {[
          ["Home",Home,"dashboard"],
          ["Index Scan",Crosshair,"index"],
          ["Stock Scan",ScanLine,"stocks"],
          ["Signals",BellRing,"signals"],
          ["Positions",BriefcaseBusiness,"positions"],
          ["Settings",Settings,"settings"]
        ].map(([label,Icon,key])=>(
          <button className={activeSection===key?"active":""} key={label} onClick={()=>goTo(key)}>
            <Icon size={23}/><span>{label}</span>
          </button>
        ))}
        <div className="side-status"><Radio size={15}/><span>{status.feed_connected?"Connected":"Disconnected"}</span></div>
      </aside>

      <main className="main-stage">
        <section className="top-grid scroll-target" ref={sectionRefs.dashboard}>
          <div ref={sectionRefs.index} className="scroll-target"><ScannerControl scanners={scanners} busy={scannerBusy} action={scannerAction} engineAction={engineAction}/></div>

          <SentimentGauge signals={signals}/>

          <div className="feed-card glass-card mobile-compact-card">
            <div className="box-title">LIVE FEED</div>
            <div className="feed-value">{status.feed_connected?"CONNECTED":"OFFLINE"}</div>
            <small>{status.feed_connected ? (status.last_tick_at ? new Date(status.last_tick_at).toLocaleTimeString("en-IN") : "Waiting for first tick") : (status.last_error || "TOTP login required")}</small>
            {showLogin && (
              <form className="otp-inline" onSubmit={connectBroker}>
                <input value={totp} maxLength={6} inputMode="numeric" placeholder="TOTP"
                  onChange={e=>setTotp(e.target.value.replace(/\D/g,"").slice(0,6))}/>
                <button disabled={busy||totp.length!==6}>{busy?"...":"CONNECT"}</button>
              </form>
            )}
            {loginError && <div className="tiny-error">{loginError}</div>}
            <button
              type="button"
              className={`telegram-check ${telegramCheck.state}`}
              disabled={telegramCheck.state === "checking"}
              onClick={checkTelegram}
              title="Sends one Telegram test message"
            >
              <BellRing size={13}/> {telegramCheck.text}
            </button>
          </div>
        </section>

        <section className={`middle-grid chart-free scroll-target ${hasKeyLevels ? "" : "no-levels"}`} ref={sectionRefs.signals}>
          <div className="signal-workspace glass-card mobile-priority">
            <div className="workspace-head">
              <div>
                <small>LIVE SIGNAL WORKSPACE</small>
                <h2>NIFTY 50 + SENSEX</h2>
              </div>
              <div className="workspace-live">
                <Radio size={15}/>
                {status.feed_connected ? "REAL-TIME" : "WAITING"}
              </div>
            </div>

            <div className="workspace-market">
              <div>
                <small>NIFTY 50</small>
                <strong>{n(feed["NIFTY 50"]?.ltp)}</strong>
                <span className={Number(feed["NIFTY 50"]?.change || 0) >= 0 ? "pos" : "neg"}>
                  {feed["NIFTY 50"]?.change == null ? "Waiting for tick" : `${Number(feed["NIFTY 50"]?.change) >= 0 ? "+" : ""}${n(feed["NIFTY 50"]?.change)}`}
                </span>
              </div>
              <div>
                <small>SENSEX</small>
                <strong>{n(feed["SENSEX"]?.ltp)}</strong>
                <span className={Number(feed["SENSEX"]?.change || 0) >= 0 ? "pos" : "neg"}>
                  {feed["SENSEX"]?.change == null ? "Waiting for tick" : `${Number(feed["SENSEX"]?.change) >= 0 ? "+" : ""}${n(feed["SENSEX"]?.change)}`}
                </span>
              </div>
            </div>

            <div className="workspace-signals">
              <div className={`workspace-signal ${signals["NIFTY 50"]?.direction === "CALL" ? "call" : signals["NIFTY 50"]?.direction === "PUT" ? "put" : "wait"}`}>
                <div>
                  <small>NIFTY SIGNAL</small>
                  <h3>{signals["NIFTY 50"]?.direction || "SCANNING"}</h3>
                  <span>{signals["NIFTY 50"]?.grade || "WAIT"}</span>
                </div>
                <div className="workspace-score">
                  <small>SCORE</small>
                  <b>{signals["NIFTY 50"]?.score ?? "—"}</b>
                </div>
              </div>

              <div className={`workspace-signal ${signals["SENSEX"]?.direction === "CALL" ? "call" : signals["SENSEX"]?.direction === "PUT" ? "put" : "wait"}`}>
                <div>
                  <small>SENSEX SIGNAL</small>
                  <h3>{signals["SENSEX"]?.direction || "SCANNING"}</h3>
                  <span>{signals["SENSEX"]?.grade || "WAIT"}</span>
                </div>
                <div className="workspace-score">
                  <small>SCORE</small>
                  <b>{signals["SENSEX"]?.score ?? "—"}</b>
                </div>
              </div>
            </div>

            <div className="workspace-confirmations">
              <div>
                <small>NIFTY CONFIRMATIONS</small>
                <div className="chip-list">
                  {(signals["NIFTY 50"]?.reasons || []).slice(0,6).map((r,i)=><span key={i}>✓ {r}</span>)}
                  {!(signals["NIFTY 50"]?.reasons || []).length && <span>Waiting for completed candles…</span>}
                </div>
              </div>
              <div>
                <small>SENSEX CONFIRMATIONS</small>
                <div className="chip-list">
                  {(signals["SENSEX"]?.reasons || []).slice(0,6).map((r,i)=><span key={i}>✓ {r}</span>)}
                  {!(signals["SENSEX"]?.reasons || []).length && <span>Waiting for completed candles…</span>}
                </div>
              </div>
            </div>
          </div>

          <KeyLevels snapshot={niftyDisplaySnapshot}/>
          <ScalpingSignal
            indexSignals={signals}
            stockSignals={stockSignals}
            onOrder={openManualOrder}
            nowMs={clockNow}
          />
        </section>

        <IndicatorStrip snapshot={niftyDisplaySnapshot}/>

        <section className="bottom-grid scroll-target">
          {(scanners?.stocks?.running || scanners?.stocks?.enabled || scanners?.stocks?.last_error) && (
            <div ref={sectionRefs.stocks} className="scroll-target"><div className="bottom-card glass-card scanner-summary">
              <div className="box-title">STOCK SCANNER</div>
              <div className="scanner-summary-main">
                <b>{scanners?.stocks?.running ? `${scanners?.stocks?.resolved || 0}/40 LIVE` : "STARTING"}</b>
                <span>Results are ranked in SCALPING SIGNALS</span>
              </div>
            </div></div>
          )}
          <RecentSignals history={combinedHistory}/>
          <div ref={sectionRefs.positions} className="scroll-target"><Positions positions={positions} loading={positionsLoading} refresh={loadPositions}
            squareOff={squareOff} squareOffAll={squareOffAll}/></div>
        </section>

        <div ref={sectionRefs.settings} className="settings-strip glass-card scroll-target">
          <Settings size={15}/>
          <span>MANUAL ORDER MODE</span>
          <b>Auto execution OFF</b>
          <span>Live data + scanners + positions</span>
        </div>

        {message && <div className="status-toast">{message}</div>}
      </main>

      <nav className="mobile-quickbar">
        <button type="button" onClick={()=>goTo("signals")}>
          <Zap size={16}/>
          <span>Signals</span>
        </button>
        <button type="button" onClick={()=>goTo("positions")}>
          <BriefcaseBusiness size={16}/>
          <span>Positions</span>
        </button>
        <button type="button" onClick={()=>goTo("dashboard")}>
          <Play size={16}/>
          <span>Scan</span>
        </button>
      </nav>

      <ManualOrderModal draft={orderDraft} busy={orderBusy} close={()=>setOrderDraft(null)} submit={submitManualOrder}/>
    </div>
  );
}

createRoot(document.getElementById("root")).render(<App />);
