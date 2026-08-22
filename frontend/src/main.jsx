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
  CircleDot
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
              isDown ? "▼" : "▲"
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

  /*
   * IMPORTANT:
   * Login error is kept separately.
   *
   * Even if websocket/backend state briefly
   * reports broker_connected=true,
   * a failed TOTP keeps the login panel visible.
   */
  const [loginError, setLoginError] =
    React.useState("");


  React.useEffect(() => {

    let ws;
    let reconnectTimer;
    let pingTimer;
    let destroyed = false;


    const connectBrowserSocket = () => {

      if (destroyed) {
        return;
      }

      try {

        ws = new WebSocket(
          WS_URL
        );

      } catch (error) {

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

            /*
             * Successful backend session:
             * clear any old login error.
             */
            if (
              msg.data
                ?.broker_connected
            ) {

              setLoginError("");

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

        } catch (error) {

          console.error(
            "WebSocket message error:",
            error
          );

        }
      };


      ws.onerror = () => {
        /*
         * onclose will handle retry.
         */
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
    };


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


    /*
     * Browser-side TOTP validation.
     */
    if (
      !/^\d{6}$/.test(totp)
    ) {

      setLoginError(
        "Enter the current 6-digit TOTP."
      );

      setMessage(
        "Enter the current 6-digit TOTP."
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


      /*
       * Safely parse backend response.
       */
      let data = {};

      try {

        data =
          await response.json();

      } catch (_) {

        data = {};

      }


      /*
       * Wrong TOTP / MPIN / API error
       */
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


      /*
       * SUCCESS
       */
      setLoginError("");

      setMessage(
        "Kotak authenticated. Waiting for real-time ticks..."
      );

      setTotp("");


      /*
       * Immediate local UI update.
       * WebSocket status will confirm afterward.
       */
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

      const text =
        error?.message ||
        String(error) ||
        "Kotak connection failed.";


      /*
       * CRITICAL FIX:
       *
       * Failed TOTP MUST NOT hide
       * the login panel.
       */
      setLoginError(
        text
      );

      setMessage(
        `Login failed: ${text}`
      );


      /*
       * Clear bad/expired TOTP.
       */
      setTotp("");


      /*
       * Force local broker state false.
       * This prevents stale UI state
       * from hiding the TOTP box.
       */
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


  /*
   * IMPORTANT FIX:
   *
   * Login panel stays visible when:
   * 1. broker is disconnected
   * OR
   * 2. last login attempt failed.
   */
  const showLoginPanel =
    !status.broker_connected ||
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


          <div
            className=
              "nav disabled"
          >

            <Zap />

            Strategies

            <span className="soon">
              SOON
            </span>

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

                autoComplete="one-time-code"

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


                    /*
                     * User started entering
                     * a fresh TOTP.
                     */
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
