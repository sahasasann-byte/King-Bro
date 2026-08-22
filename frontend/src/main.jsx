import React from "react";
import { createRoot } from "react-dom/client";
import {
  Crown,
  LayoutDashboard,
  Activity,
  Wifi,
  WifiOff,
  LockKeyhole,
  Zap,
  Radio,
  CircleDot,
  Play,
  Square,
  ScanLine,
  CandlestickChart
} from "lucide-react";

import "./styles.css";

const API =
  import.meta.env.VITE_API_URL ||
  "http://localhost:8000";

const WS_URL =
  API.replace(/^http/, "ws") +
  "/ws/market";

const EMPTY = {
  "NIFTY 50": {
    key: "NIFTY 50",
    ltp: null,
  },

  "SENSEX": {
    key: "SENSEX",
    ltp: null,
  },

  "BANK NIFTY": {
    key: "BANK NIFTY",
    ltp: null,
  },
};


function formatNumber(value) {
  if (
    value === null ||
    value === undefined ||
    Number.isNaN(Number(value))
  ) {
    return "—";
  }

  return Number(value).toLocaleString(
    "en-IN",
    {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }
  );
}


function MarketCard({ item }) {
  const pct = item.percent_change;
  const change = item.change;

  const isDown =
    pct !== null &&
    pct !== undefined &&
    Number(pct) < 0;

  return (
    <div className="market-card glass">

      <div className="card-shine" />

      <div className="market-title">

        <div>
          <h3>{item.key}</h3>

          <small>
            {item.key === "SENSEX"
              ? "BSE"
              : "NSE"}
          </small>
        </div>

        <div
          className={
            `pulse-icon ${
              isDown
                ? "down"
                : "up"
            }`
          }
        >
          <Activity />
        </div>

      </div>


      <div className="market-price">
        {formatNumber(item.ltp)}
      </div>


      <div
        className={
          isDown
            ? "market-change down"
            : "market-change up"
        }
      >
        {change == null
          ? "Waiting for Kotak live tick"

          : `${
              isDown
                ? "▼"
                : "▲"
            } ${
              formatNumber(
                Math.abs(change)
              )
            }${
              pct == null
                ? ""

                : ` (${
                    isDown
                      ? ""
                      : "+"
                  }${formatNumber(pct)}%)`
            }`
        }
      </div>


      <div className="mini-wave">
        <span />
      </div>


      <div className="tick-time">

        {item.received_at
          ? `LIVE • ${
              new Date(
                item.received_at
              ).toLocaleTimeString(
                "en-IN"
              )
            }`

          : "No live tick received"
        }

      </div>

    </div>
  );
}


function ScannerPanel({
  scanners,
  scannerBusy,
  onIndexStart,
  onIndexStop,
  onStockStart,
  onStockStop
}) {
  const indexOn =
    Boolean(
      scanners?.index?.enabled
    );

  const stockEnabled =
    Boolean(
      scanners?.stocks?.enabled
    );

  const stockRunning =
    Boolean(
      scanners?.stocks?.running
    );

  const panelStyle = {
    marginTop: "18px",
    padding: "18px",
  };

  const gridStyle = {
    display: "grid",
    gridTemplateColumns:
      "repeat(auto-fit, minmax(260px, 1fr))",
    gap: "14px",
  };

  const cardStyle = {
    padding: "16px",
    borderRadius: "18px",
    border:
      "1px solid rgba(120,255,220,0.12)",
    background:
      "rgba(5,25,28,0.28)",
    backdropFilter: "blur(14px)",
  };

  const headStyle = {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: "12px",
    marginBottom: "12px",
  };

  const statusStyle = (active) => ({
    fontSize: "11px",
    fontWeight: 800,
    letterSpacing: "0.08em",
    color: active
      ? "#69ffbf"
      : "#ff7f92",
  });

  const btnRow = {
    display: "flex",
    gap: "10px",
    marginTop: "14px",
    flexWrap: "wrap",
  };

  const btnBase = {
    border: "1px solid rgba(115,255,225,0.22)",
    background:
      "rgba(0, 255, 190, 0.08)",
    color: "#dffef7",
    padding: "10px 14px",
    borderRadius: "12px",
    fontWeight: 800,
    cursor: "pointer",
    display: "inline-flex",
    alignItems: "center",
    gap: "7px",
  };

  return (
    <section
      className="overview glass"
      style={panelStyle}
    >

      <div className="overview-head">

        <div>
          <h3>
            Signal Scanner Control
          </h3>

          <small className="green">
            ● V7 backend scanner controls
          </small>
        </div>

        <div className="live-badge">
          <ScanLine />
          SCANNER
        </div>

      </div>


      <div style={gridStyle}>

        <div style={cardStyle}>

          <div style={headStyle}>

            <div>
              <b>
                INDEX SIG
              </b>

              <div
                style={{
                  marginTop: "4px",
                  fontSize: "12px",
                  opacity: 0.7,
                }}
              >
                NIFTY 50 + SENSEX
              </div>
            </div>

            <span
              style={
                statusStyle(indexOn)
              }
            >
              {indexOn
                ? "RUNNING"
                : "STOPPED"}
            </span>

          </div>


          <div
            style={{
              fontSize: "12px",
              lineHeight: 1.6,
              opacity: 0.78,
            }}
          >
            1M + 5M + 15M signal
            engine with option confirmation.
          </div>


          <div style={btnRow}>

            <button
              type="button"
              style={btnBase}
              disabled={
                scannerBusy ===
                "index-start"
              }
              onClick={onIndexStart}
            >
              <Play size={15} />
              START
            </button>

            <button
              type="button"
              style={{
                ...btnBase,
                background:
                  "rgba(255,80,110,0.08)",
                border:
                  "1px solid rgba(255,110,130,0.22)",
              }}
              disabled={
                scannerBusy ===
                "index-stop"
              }
              onClick={onIndexStop}
            >
              <Square size={15} />
              STOP
            </button>

          </div>

        </div>


        <div style={cardStyle}>

          <div style={headStyle}>

            <div>
              <b>
                STOCK SIG
              </b>

              <div
                style={{
                  marginTop: "4px",
                  fontSize: "12px",
                  opacity: 0.7,
                }}
              >
                Midcap + Smallcap universe
              </div>
            </div>

            <span
              style={
                statusStyle(
                  stockRunning
                )
              }
            >
              {stockRunning
                ? "RUNNING"
                : stockEnabled
                  ? "ARMED"
                  : "STOPPED"}
            </span>

          </div>


          <div
            style={{
              fontSize: "12px",
              lineHeight: 1.6,
              opacity: 0.78,
            }}
          >
            Control is ready.
            Actual stock universe feed
            will be wired in the next
            backend module.
          </div>


          <div style={btnRow}>

            <button
              type="button"
              style={btnBase}
              disabled={
                scannerBusy ===
                "stock-start"
              }
              onClick={onStockStart}
            >
              <Play size={15} />
              START
            </button>

            <button
              type="button"
              style={{
                ...btnBase,
                background:
                  "rgba(255,80,110,0.08)",
                border:
                  "1px solid rgba(255,110,130,0.22)",
              }}
              disabled={
                scannerBusy ===
                "stock-stop"
              }
              onClick={onStockStop}
            >
              <Square size={15} />
              STOP
            </button>

          </div>

        </div>

      </div>

    </section>
  );
}


function SignalCard({ symbol, signal }) {
  const noSignal = !signal;

  const direction = signal?.direction || "WAIT";
  const grade = signal?.grade || "SCANNING";
  const score = signal?.score ?? null;
  const actionable = Boolean(signal?.actionable);

  const isCall = direction === "CALL";
  const isPut = direction === "PUT";

  const accent =
    isCall
      ? "#69ffbf"
      : isPut
        ? "#ff7f92"
        : "#8edfff";

  const cardStyle = {
    padding: "18px",
    borderRadius: "20px",
    border: `1px solid ${accent}33`,
    background: "rgba(4, 22, 29, 0.34)",
    backdropFilter: "blur(16px)",
    boxShadow: `0 0 35px ${accent}10`,
  };

  const pillStyle = {
    display: "inline-flex",
    alignItems: "center",
    gap: "8px",
    padding: "7px 11px",
    borderRadius: "999px",
    border: `1px solid ${accent}44`,
    color: accent,
    fontSize: "12px",
    fontWeight: 900,
    letterSpacing: "0.08em",
  };

  const row = {
    display: "grid",
    gridTemplateColumns: "repeat(4, minmax(0, 1fr))",
    gap: "10px",
    marginTop: "14px",
  };

  const stat = {
    padding: "11px",
    borderRadius: "14px",
    background: "rgba(255,255,255,0.035)",
    border: "1px solid rgba(255,255,255,0.06)",
  };

  const small = {
    fontSize: "10px",
    opacity: 0.62,
    letterSpacing: "0.08em",
    marginBottom: "5px",
  };

  const value = {
    fontSize: "14px",
    fontWeight: 800,
  };

  const optionName =
    signal?.option_contract?.display_symbol ||
    "Waiting for option confirmation";

  const reasons =
    Array.isArray(signal?.reasons)
      ? signal.reasons.slice(0, 6)
      : [];

  const blockers =
    Array.isArray(signal?.blockers)
      ? signal.blockers.slice(0, 4)
      : [];

  return (
    <div style={cardStyle}>

      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "flex-start",
          gap: "14px",
        }}
      >
        <div>
          <div
            style={{
              fontSize: "12px",
              opacity: 0.65,
              marginBottom: "5px",
            }}
          >
            {symbol} SIGNAL
          </div>

          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: "10px",
              flexWrap: "wrap",
            }}
          >
            <h3
              style={{
                margin: 0,
                color: accent,
                fontSize: "24px",
              }}
            >
              {noSignal
                ? "SCANNING"
                : direction}
            </h3>

            <span style={pillStyle}>
              {grade}
            </span>
          </div>
        </div>

        <div
          style={{
            textAlign: "right",
          }}
        >
          <div
            style={{
              fontSize: "10px",
              opacity: 0.62,
              letterSpacing: "0.08em",
            }}
          >
            SCORE
          </div>

          <div
            style={{
              color: accent,
              fontSize: "26px",
              fontWeight: 900,
            }}
          >
            {score == null ? "—" : score}
          </div>
        </div>
      </div>


      <div
        style={{
          marginTop: "12px",
          fontSize: "13px",
          opacity: 0.82,
        }}
      >
        {noSignal
          ? "Waiting for enough completed candles and a valid setup."
          : actionable
            ? `Actionable setup • ${optionName}`
            : `${optionName}`}
      </div>


      <div style={row}>

        <div style={stat}>
          <div style={small}>
            OPTION LTP
          </div>
          <div style={value}>
            {signal?.option_ltp == null
              ? "—"
              : `₹${formatNumber(signal.option_ltp)}`}
          </div>
        </div>

        <div style={stat}>
          <div style={small}>
            ENTRY
          </div>
          <div style={value}>
            {signal?.entry == null
              ? "—"
              : `₹${formatNumber(signal.entry)}`}
          </div>
        </div>

        <div style={stat}>
          <div style={small}>
            STOP LOSS
          </div>
          <div style={value}>
            {signal?.stop_loss == null
              ? "—"
              : `₹${formatNumber(signal.stop_loss)}`}
          </div>
        </div>

        <div style={stat}>
          <div style={small}>
            T1 / T2
          </div>
          <div style={value}>
            {signal?.target_1 == null
              ? "—"
              : `₹${formatNumber(signal.target_1)} / ₹${formatNumber(signal.target_2)}`}
          </div>
        </div>

      </div>


      <div
        style={{
          marginTop: "14px",
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: "12px",
        }}
      >

        <div
          style={{
            padding: "12px",
            borderRadius: "14px",
            background: "rgba(0,255,190,0.035)",
          }}
        >
          <div
            style={{
              fontSize: "10px",
              opacity: 0.62,
              marginBottom: "7px",
              letterSpacing: "0.08em",
            }}
          >
            CONFIRMATIONS
          </div>

          {reasons.length ? (
            reasons.map((reason, index) => (
              <div
                key={index}
                style={{
                  fontSize: "12px",
                  lineHeight: 1.6,
                  color: "#bfffe8",
                }}
              >
                ✓ {reason}
              </div>
            ))
          ) : (
            <div
              style={{
                fontSize: "12px",
                opacity: 0.65,
              }}
            >
              Waiting for confirmations...
            </div>
          )}
        </div>


        <div
          style={{
            padding: "12px",
            borderRadius: "14px",
            background: "rgba(255,90,120,0.035)",
          }}
        >
          <div
            style={{
              fontSize: "10px",
              opacity: 0.62,
              marginBottom: "7px",
              letterSpacing: "0.08em",
            }}
          >
            BLOCKERS
          </div>

          {blockers.length ? (
            blockers.map((blocker, index) => (
              <div
                key={index}
                style={{
                  fontSize: "12px",
                  lineHeight: 1.6,
                  color: "#ffc2cc",
                }}
              >
                • {blocker}
              </div>
            ))
          ) : (
            <div
              style={{
                fontSize: "12px",
                color: "#bfffe8",
              }}
            >
              No active blocker
            </div>
          )}
        </div>

      </div>

    </div>
  );
}


function App() {

  const [feed, setFeed] =
    React.useState(EMPTY);

  const [status, setStatus] =
    React.useState({
      broker_connected: false,
      feed_connected: false,
      last_tick_at: null,
      last_error: null,
    });

  const [totp, setTotp] =
    React.useState("");

  const [busy, setBusy] =
    React.useState(false);

  const [message, setMessage] =
    React.useState("");

  const [loginError, setLoginError] =
    React.useState("");

  const [scanners, setScanners] =
    React.useState({
      index: {
        enabled: true,
        symbols: [
          "NIFTY 50",
          "SENSEX",
        ],
      },

      stocks: {
        enabled: false,
        running: false,
      },
    });

  const [
    scannerBusy,
    setScannerBusy
  ] = React.useState("");


  const [signals, setSignals] =
    React.useState({
      "NIFTY 50": null,
      "SENSEX": null,
    });


  const loadScanners =
    React.useCallback(
      async () => {

        try {

          const response =
            await fetch(
              API +
                "/api/scanners"
            );

          if (!response.ok) {
            return;
          }

          const data =
            await response.json();

          setScanners(data);

        } catch (_) {
          // Keep UI alive if backend is sleeping.
        }

      },
      []
    );



  const loadSignals =
    React.useCallback(
      async () => {

        try {

          const response =
            await fetch(
              API +
                "/api/signals"
            );

          if (!response.ok) {
            return;
          }

          const data =
            await response.json();

          const backendSignals =
            data?.signals || {};

          setSignals({
            "NIFTY 50":
              backendSignals["NIFTY 50"] || null,

            "SENSEX":
              backendSignals["SENSEX"] || null,
          });

        } catch (_) {
          // Signal cards continue showing last known data.
        }

      },
      []
    );


  React.useEffect(() => {

    loadScanners();
    loadSignals();

    const timer =
      setInterval(
        loadSignals,
        15000
      );

    return () =>
      clearInterval(timer);

  }, [loadScanners, loadSignals]);


  React.useEffect(() => {

    let ws = null;
    let reconnectTimer = null;
    let pingTimer = null;
    let destroyed = false;


    function connectBrowserSocket() {

      if (destroyed) {
        return;
      }


      try {

        ws = new WebSocket(
          WS_URL
        );

      } catch (_) {

        reconnectTimer =
          setTimeout(
            connectBrowserSocket,
            3000
          );

        return;
      }


      ws.onopen = () => {

        try {
          ws.send("hello");
        } catch (_) {
          // ignore
        }


        clearInterval(
          pingTimer
        );


        pingTimer =
          setInterval(() => {

            if (
              ws &&
              ws.readyState ===
                WebSocket.OPEN
            ) {

              try {
                ws.send("ping");
              } catch (_) {
                // ignore
              }

            }

          }, 20000);

      };


      ws.onmessage = (event) => {

        try {

          const msg =
            JSON.parse(
              event.data
            );


          if (
            msg.type ===
            "status"
          ) {

            setStatus(
              msg.data || {}
            );


            if (
              msg.data
                ?.feed_connected
            ) {

              setLoginError("");

              setMessage(
                "Kotak live feed connected."
              );

            }

          }


          if (
            msg.type ===
            "snapshot"
          ) {

            const next = {
              ...EMPTY,
            };


            const rows =
              Array.isArray(
                msg.data
              )
                ? msg.data
                : [];


            rows.forEach(
              (item) => {

                if (
                  item?.key &&
                  next[item.key]
                ) {

                  next[item.key] =
                    item;

                }

              }
            );


            setFeed(next);

          }


          if (
            msg.type ===
            "tick"
          ) {

            const item =
              msg.data;


            if (
              item?.key &&
              Object
                .prototype
                .hasOwnProperty
                .call(
                  EMPTY,
                  item.key
                )
            ) {

              setFeed(
                (prev) => ({
                  ...prev,
                  [item.key]:
                    item,
                })
              );

            }

          }


          if (
            msg.type === "signal_update" ||
            msg.type === "signal_event"
          ) {

            const signal =
              msg.data;

            if (
              signal?.symbol &&
              Object.prototype.hasOwnProperty.call(
                {
                  "NIFTY 50": true,
                  "SENSEX": true,
                },
                signal.symbol
              )
            ) {

              setSignals(
                (prev) => ({
                  ...prev,
                  [signal.symbol]:
                    signal,
                })
              );

            }

          }

        } catch (error) {

          console.error(
            "WebSocket message error:",
            error
          );

        }

      };


      ws.onerror = () => {
        // onclose handles retry
      };


      ws.onclose = () => {

        clearInterval(
          pingTimer
        );


        if (
          !destroyed
        ) {

          reconnectTimer =
            setTimeout(
              connectBrowserSocket,
              3000
            );

        }

      };

    }


    connectBrowserSocket();


    return () => {

      destroyed = true;

      clearInterval(
        pingTimer
      );

      clearTimeout(
        reconnectTimer
      );


      try {
        ws?.close();
      } catch (_) {
        // ignore
      }

    };

  }, []);


  async function connectKotak(
    event
  ) {

    event.preventDefault();


    if (
      !/^\d{6}$/.test(totp)
    ) {

      const errorText =
        "Enter the current 6-digit TOTP.";

      setLoginError(
        errorText
      );

      setMessage(
        errorText
      );

      return;
    }


    setBusy(true);

    setLoginError("");

    setMessage(
      "Authenticating with Kotak Neo..."
    );


    try {

      const response =
        await fetch(
          API +
            "/api/kotak/connect",
          {
            method: "POST",

            headers: {
              "Content-Type":
                "application/json",
            },

            body:
              JSON.stringify({
                totp,
              }),
          }
        );


      let data = {};

      try {

        data =
          await response.json();

      } catch (_) {

        data = {};

      }


      if (!response.ok) {

        const detail =
          data?.detail;

        let errorMessage =
          "Kotak authentication failed.";


        if (
          typeof detail ===
          "string"
        ) {

          errorMessage =
            detail;

        } else if (
          detail &&
          typeof detail ===
            "object"
        ) {

          errorMessage =
            detail.message ||
            detail.error ||
            detail.error_type ||
            errorMessage;

        }


        throw new Error(
          errorMessage
        );

      }


      setLoginError("");

      setMessage(
        "Kotak authenticated. Starting live feed..."
      );


      setTotp("");


      setStatus(
        (prev) => ({
          ...prev,

          broker_connected:
            true,

          last_error:
            null,
        })
      );


    } catch (error) {

      const errorText =
        error?.message ||
        String(error) ||
        "Kotak connection failed.";


      setLoginError(
        errorText
      );


      setMessage(
        `Login failed: ${errorText}`
      );


      setTotp("");


      setStatus(
        (prev) => ({
          ...prev,

          broker_connected:
            false,

          feed_connected:
            false,
        })
      );


    } finally {

      setBusy(false);

    }

  }


  async function scannerAction(
    path,
    busyName
  ) {

    setScannerBusy(
      busyName
    );

    try {

      const response =
        await fetch(
          API + path,
          {
            method: "POST",
          }
        );

      const data =
        await response.json();

      if (
        data?.message
      ) {
        setMessage(
          data.message
        );
      } else {
        setMessage(
          "Scanner state updated."
        );
      }

      await loadScanners();

    } catch (error) {

      setMessage(
        `Scanner error: ${
          error?.message ||
          String(error)
        }`
      );

    } finally {

      setScannerBusy("");

    }

  }


  const showLoginPanel =
    !status.feed_connected ||
    Boolean(loginError);


  return (

    <div className="page">


      <div className="ambient-lights">

        <span
          className=
            "light light1"
        />

        <span
          className=
            "light light2"
        />

        <span
          className=
            "light light3"
        />

      </div>


      <div className="grid-overlay" />


      <aside
        className=
          "sidebar glass"
      >

        <div className="brand">

          <div
            className=
              "brand-icon"
          >
            <Crown />
          </div>


          <h2>
            KING
          </h2>


          <h2 className="bro">
            BRO
          </h2>


          <span>
            TERMINAL
          </span>

        </div>


        <nav>

          <div className="nav active">
            <LayoutDashboard />
            Dashboard
          </div>


          <div className="nav">
            <Activity />
            Live Market
          </div>


          <div className="nav">
            <CandlestickChart />
            Strategies
          </div>

        </nav>


        <div
          className=
            "connection glass"
        >

          {status.feed_connected
            ? <Wifi />
            : <WifiOff />
          }


          <div>

            <b>

              {status.feed_connected
                ? "CONNECTED"
                : "DISCONNECTED"
              }

            </b>


            <small>
              Kotak Neo SFeed
            </small>


            <small>
              No mock data
            </small>

          </div>

        </div>

      </aside>


      <main>


        <header>

          <div>

            <h1>
              King Bro{" "}
              <span>
                Terminal
              </span>
            </h1>


            <p>
              Real Market Data
              {" • "}
              Kotak Neo
              {" • "}
              No Mock Prices
            </p>

          </div>


          <div
            className={
              status.feed_connected
                ? "live-pill live"
                : "live-pill off"
            }
          >

            <CircleDot />


            {status.feed_connected
              ? "LIVE"
              : "OFFLINE"
            }

          </div>

        </header>


        {showLoginPanel && (

          <form
            className=
              "totp-panel glass"

            onSubmit=
              {connectKotak}
          >

            <div className="login-copy">

              <LockKeyhole />


              <div>

                <h3>
                  Morning Kotak Login
                </h3>


                <p>
                  Consumer Key,
                  Mobile, UCC and MPIN
                  are already stored
                  on the backend.
                </p>

              </div>

            </div>


            <label>

              CURRENT 6-DIGIT TOTP


              <input
                type="text"

                inputMode="numeric"

                autoComplete=
                  "one-time-code"

                maxLength={6}

                value={totp}

                disabled={busy}

                onChange={
                  (event) => {

                    const clean =
                      event
                        .target
                        .value
                        .replace(
                          /\D/g,
                          ""
                        )
                        .slice(
                          0,
                          6
                        );


                    setTotp(
                      clean
                    );


                    if (
                      loginError
                    ) {

                      setLoginError(
                        ""
                      );

                    }

                  }
                }

                placeholder="123456"

                autoFocus
              />

            </label>


            <button
              type="submit"

              disabled={
                busy ||
                totp.length !== 6
              }
            >

              {busy
                ? "CONNECTING..."
                : "CONNECT LIVE DATA"
              }

            </button>


            {loginError && (

              <div
                style={{
                  width: "100%",
                  marginTop: "10px",
                  color: "#ff6b7a",
                  fontSize: "12px",
                  fontWeight: 600,
                }}
              >

                ⚠ {loginError}

              </div>

            )}

          </form>

        )}


        <section className="cards">

          <MarketCard
            item={
              feed[
                "NIFTY 50"
              ]
            }
          />


          <MarketCard
            item={
              feed[
                "SENSEX"
              ]
            }
          />


          <MarketCard
            item={
              feed[
                "BANK NIFTY"
              ]
            }
          />

        </section>


        <ScannerPanel
          scanners={scanners}
          scannerBusy={
            scannerBusy
          }

          onIndexStart={() =>
            scannerAction(
              "/api/scanners/index/start",
              "index-start"
            )
          }

          onIndexStop={() =>
            scannerAction(
              "/api/scanners/index/stop",
              "index-stop"
            )
          }

          onStockStart={() =>
            scannerAction(
              "/api/scanners/stocks/start",
              "stock-start"
            )
          }

          onStockStop={() =>
            scannerAction(
              "/api/scanners/stocks/stop",
              "stock-stop"
            )
          }
        />



        <section
          className="overview glass"
          style={{
            marginTop: "18px",
            padding: "18px",
          }}
        >

          <div className="overview-head">

            <div>
              <h3>
                Live Index Signals
              </h3>

              <small className="green">
                ● WebSocket live signal updates
              </small>
            </div>

            <div className="live-badge">
              <Zap />
              SIGNALS
            </div>

          </div>


          <div
            style={{
              display: "grid",
              gridTemplateColumns:
                "repeat(auto-fit, minmax(320px, 1fr))",
              gap: "14px",
            }}
          >

            <SignalCard
              symbol="NIFTY 50"
              signal={
                signals["NIFTY 50"]
              }
            />

            <SignalCard
              symbol="SENSEX"
              signal={
                signals["SENSEX"]
              }
            />

          </div>

        </section>


        <section
          className=
            "overview glass"
        >


          <div
            className=
              "overview-head"
          >

            <div>

              <h3>
                Live Market Monitor
              </h3>


              <small
                className={
                  status.feed_connected
                    ? "green"
                    : "red"
                }
              >

                ●{" "}


                {status.feed_connected
                  ? "Kotak live feed connected"
                  : "Waiting for live feed"
                }

              </small>

            </div>


            <div className="live-badge">

              <Radio />

              REAL-TIME

            </div>

          </div>


          <div className="monitor">


            <div className="monitor-orb">
              <Activity />
            </div>


            {status.feed_connected
              ? (
                <>

                  <b>
                    Real-time ticks are arriving from Kotak Neo.
                  </b>


                  <span>
                    NIFTY 50
                    {" • "}
                    SENSEX
                    {" • "}
                    BANK NIFTY
                  </span>

                </>
              )

              : (
                <>

                  <b>
                    No live market tick received.
                  </b>


                  <span>
                    Enter the current TOTP above to start the Kotak session.
                  </span>

                </>
              )
            }

          </div>

        </section>


        <div className="message">

          {message}


          {status.last_error &&
           !loginError
            ? ` • ${status.last_error}`
            : ""
          }

        </div>


      </main>

    </div>
  );
}


createRoot(
  document.getElementById(
    "root"
  )
).render(
  <App />
);
