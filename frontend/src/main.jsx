import React from "react";
import { createRoot } from "react-dom/client";
import {
  Crown,
  LayoutDashboard,
  Activity,
  Wifi,
  WifiOff,
  LockKeyhole,
} from "lucide-react";
import "./styles.css";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";
const WS_URL = API.replace(/^http/, "ws") + "/ws/market";

const EMPTY = {
  "NIFTY 50": { key: "NIFTY 50", ltp: null },
  SENSEX: { key: "SENSEX", ltp: null },
  "BANK NIFTY": { key: "BANK NIFTY", ltp: null },
};

function formatNumber(value) {
  if (
    value === null ||
    value === undefined ||
    Number.isNaN(Number(value))
  ) {
    return "—";
  }

  return Number(value).toLocaleString("en-IN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function MarketCard({ item }) {
  const pct = item.percent_change;

  const isDown =
    pct !== null &&
    pct !== undefined &&
    Number(pct) < 0;

  return (
    <div className="market-card glass">
      <div className="market-title">
        <div>
          <h3>{item.key}</h3>

          <small>
            {item.key === "SENSEX" ? "BSE" : "NSE"}
          </small>
        </div>

        <Activity
          className={isDown ? "down" : "up"}
        />
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
        {item.change == null
          ? "Waiting for Kotak live tick"
          : `${isDown ? "▼" : "▲"} ${formatNumber(
              Math.abs(item.change)
            )}${
              pct == null
                ? ""
                : ` (${
                    isDown ? "" : "+"
                  }${formatNumber(pct)}%)`
            }`}
      </div>

      <div className="real-feed-line">
        <span />
      </div>

      <div className="tick-time">
        {item.received_at
          ? `LIVE • ${new Date(
              item.received_at
            ).toLocaleTimeString("en-IN")}`
          : "No live tick received"}
      </div>
    </div>
  );
}

function App() {
  const [feed, setFeed] = React.useState(EMPTY);

  const [status, setStatus] = React.useState({
    broker_connected: false,
    feed_connected: false,
    last_tick_at: null,
    last_error: null,
  });

  const [totp, setTotp] = React.useState("");
  const [busy, setBusy] = React.useState(false);
  const [message, setMessage] = React.useState("");

  React.useEffect(() => {
    let ws;
    let reconnectTimer;
    let pingTimer;

    const connectBrowserSocket = () => {
      ws = new WebSocket(WS_URL);

      ws.onopen = () => {
        ws.send("hello");

        pingTimer = setInterval(() => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send("ping");
          }
        }, 20000);
      };

      ws.onmessage = (event) => {
        const msg = JSON.parse(event.data);

        if (msg.type === "status") {
          setStatus(msg.data);
        }

        if (msg.type === "snapshot") {
          const next = { ...EMPTY };

          msg.data.forEach((item) => {
            if (next[item.key]) {
              next[item.key] = item;
            }
          });

          setFeed(next);
        }

        if (msg.type === "tick") {
          const item = msg.data;

          if (
            item.key &&
            Object.prototype.hasOwnProperty.call(
              EMPTY,
              item.key
            )
          ) {
            setFeed((prev) => ({
              ...prev,
              [item.key]: item,
            }));
          }
        }
      };

      ws.onclose = () => {
        clearInterval(pingTimer);

        reconnectTimer = setTimeout(
          connectBrowserSocket,
          3000
        );
      };
    };

    connectBrowserSocket();

    return () => {
      clearInterval(pingTimer);
      clearTimeout(reconnectTimer);
      ws?.close();
    };
  }, []);

  async function connectKotak(event) {
    event.preventDefault();

    if (!/^\d{6}$/.test(totp)) {
      setMessage(
        "Enter the current 6-digit TOTP."
      );
      return;
    }

    setBusy(true);
    setMessage(
      "Authenticating with Kotak Neo..."
    );

    try {
      const response = await fetch(
        API + "/api/kotak/connect",
        {
          method: "POST",

          headers: {
            "Content-Type": "application/json",
          },

          body: JSON.stringify({
            totp,
          }),
        }
      );

      const data = await response.json();

      if (!response.ok) {
        const detail = data.detail;

        throw new Error(
          typeof detail === "object"
            ? detail.message ||
                JSON.stringify(detail)
            : detail ||
                "Kotak connection failed"
        );
      }

      setMessage(
        "Kotak authenticated. Waiting for real-time ticks..."
      );

      setTotp("");
    } catch (error) {
      setMessage(
        error.message || String(error)
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="page">
      <aside className="sidebar glass">
        <div className="brand">
          <Crown />

          <h2>KING</h2>

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
        </nav>

        <div className="connection glass">
          {status.feed_connected ? (
            <Wifi />
          ) : (
            <WifiOff />
          )}

          <div>
            <b>
              {status.feed_connected
                ? "CONNECTED"
                : "DISCONNECTED"}
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
              Real Market Data • Kotak Neo •
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
            ●{" "}
            {status.feed_connected
              ? "LIVE"
              : "OFFLINE"}
          </div>
        </header>

        {!status.broker_connected && (
          <form
            className="totp-panel glass"
            onSubmit={connectKotak}
          >
            <div className="login-copy">
              <LockKeyhole />

              <div>
                <h3>
                  Morning Kotak Login
                </h3>

                <p>
                  Consumer Key, Mobile,
                  UCC and MPIN are already
                  stored on the backend.
                </p>
              </div>
            </div>

            <label>
              CURRENT 6-DIGIT TOTP

              <input
                inputMode="numeric"
                maxLength={6}
                value={totp}
                onChange={(event) =>
                  setTotp(
                    event.target.value
                      .replace(/\D/g, "")
                      .slice(0, 6)
                  )
                }
                placeholder="123456"
              />
            </label>

            <button
              disabled={busy}
            >
              {busy
                ? "CONNECTING..."
                : "CONNECT LIVE DATA"}
            </button>
          </form>
        )}

        <section className="cards">
          <MarketCard
            item={feed["NIFTY 50"]}
          />

          <MarketCard
            item={feed["SENSEX"]}
          />

          <MarketCard
            item={feed["BANK NIFTY"]}
          />
        </section>

        <section className="overview glass">
          <div className="overview-head">
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
                  : "Waiting for live feed"}
              </small>
            </div>
          </div>

          <div className="monitor">
            {status.feed_connected ? (
              <>
                <Activity />

                <b>
                  Real-time ticks are
                  arriving from Kotak Neo.
                </b>

                <span>
                  NIFTY 50 • SENSEX • BANK
                  NIFTY
                </span>
              </>
            ) : (
              <>
                <WifiOff />

                <b>
                  No live market tick received.
                </b>

                <span>
                  Enter the current TOTP above
                  to start the Kotak session.
                </span>
              </>
            )}
          </div>
        </section>

        <div className="message">
          {message}

          {status.last_error
            ? ` • ${status.last_error}`
            : ""}
        </div>
      </main>
    </div>
  );
}

createRoot(
  document.getElementById("root")
).render(<App />);
