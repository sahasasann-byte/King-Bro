import React from "react";
import { createRoot } from "react-dom/client";
import {
  Crown,
  Activity,
  Wifi,
  WifiOff,
  Play,
  Square,
  Zap,
  BriefcaseBusiness,
  ScanLine,
  RefreshCw,
  X,
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
  "NIFTY 50": { key: "NIFTY 50", ltp: null },
  "SENSEX": { key: "SENSEX", ltp: null },
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

function MarketTile({ item }) {
  const change = Number(item?.change || 0);
  const down = change < 0;

  return (
    <div className="market-tile">
      <div className="market-name">
        <span>{item.key}</span>
        <Activity size={16} />
      </div>

      <div className="market-ltp">
        {formatNumber(item.ltp)}
      </div>

      <div
        className={
          down
            ? "market-delta down"
            : "market-delta up"
        }
      >
        {item.change == null
          ? "Waiting for live tick"
          : `${down ? "▼" : "▲"} ${formatNumber(
              Math.abs(item.change)
            )}`}
      </div>
    </div>
  );
}

function ScannerBar({
  scanners,
  busy,
  onIndexStart,
  onIndexStop,
  onStockStart,
  onStockStop
}) {
  const indexOn =
    Boolean(scanners?.index?.enabled);

  const stockOn =
    Boolean(scanners?.stocks?.running);

  return (
    <section className="scanner-bar">
      <div className="scanner-title">
        <ScanLine size={19} />
        <span>SCAN CONTROL</span>
      </div>

      <div className="scan-unit">
        <div>
          <b>INDEX SIG</b>
          <small>
            NIFTY 50 + SENSEX
          </small>
        </div>

        <span
          className={
            indexOn
              ? "state on"
              : "state off"
          }
        >
          {indexOn
            ? "RUNNING"
            : "STOPPED"}
        </span>

        <button
          className="mini-btn start"
          disabled={busy === "index-start"}
          onClick={onIndexStart}
        >
          <Play size={14} />
          START
        </button>

        <button
          className="mini-btn stop"
          disabled={busy === "index-stop"}
          onClick={onIndexStop}
        >
          <Square size={13} />
          STOP
        </button>
      </div>

      <div className="scan-divider" />

      <div className="scan-unit">
        <div>
          <b>STOCK SIG</b>
          <small>
            Fixed 40-stock universe
          </small>
        </div>

        <span
          className={
            stockOn
              ? "state on"
              : "state off"
          }
        >
          {stockOn
            ? "RUNNING"
            : "STOPPED"}
        </span>

        <button
          className="mini-btn start"
          disabled={busy === "stock-start"}
          onClick={onStockStart}
        >
          <Play size={14} />
          START
        </button>

        <button
          className="mini-btn stop"
          disabled={busy === "stock-stop"}
          onClick={onStockStop}
        >
          <Square size={13} />
          STOP
        </button>
      </div>
    </section>
  );
}

function IndexSignalCard({
  title,
  signal,
  onOrder
}) {
  const direction =
    signal?.direction || "SCANNING";

  const call =
    direction === "CALL";

  const put =
    direction === "PUT";

  const tone =
    call
      ? "positive"
      : put
        ? "negative"
        : "neutral";

  return (
    <article className={`signal-card ${tone}`}>
      <div className="signal-head">
        <div>
          <small>{title}</small>
          <h3>{direction}</h3>
        </div>

        <div className="score">
          <span>SCORE</span>
          <b>
            {signal?.score ?? "—"}
          </b>
        </div>
      </div>

      <div className="signal-meta">
        <span>
          {signal?.grade || "SCANNING"}
        </span>
        <span>
          {signal?.option_contract?.display_symbol ||
            signal?.option_contract?.trading_symbol ||
            "Waiting for setup"}
        </span>
      </div>

      <div className="levels">
        <div>
          <small>LTP</small>
          <b>
            {signal?.option_ltp == null
              ? "—"
              : `₹${formatNumber(signal.option_ltp)}`}
          </b>
        </div>

        <div>
          <small>ENTRY</small>
          <b>
            {signal?.entry == null
              ? "—"
              : `₹${formatNumber(signal.entry)}`}
          </b>
        </div>

        <div>
          <small>SL</small>
          <b>
            {signal?.stop_loss == null
              ? "—"
              : `₹${formatNumber(signal.stop_loss)}`}
          </b>
        </div>

        <div>
          <small>T1 / T2</small>
          <b>
            {signal?.target_1 == null
              ? "—"
              : `₹${formatNumber(signal.target_1)} / ₹${formatNumber(signal.target_2)}`}
          </b>
        </div>
      </div>

      <div className="signal-footer">
        <div className="reason-line">
          {(signal?.reasons || [])
            .slice(0, 4)
            .map((reason) => (
              <span key={reason}>
                ✓ {reason}
              </span>
            ))}
        </div>

        <button
          className="primary-order"
          disabled={
            !signal?.actionable ||
            !signal?.option_contract
          }
          onClick={() =>
            onOrder({
              kind: "INDEX_OPTION",
              signal,
            })
          }
        >
          MANUAL ORDER
        </button>
      </div>
    </article>
  );
}

function StockSignals({
  items,
  scanners,
  onOrder
}) {
  const shown =
    (items || []).slice(0, 6);

  return (
    <section className="panel stock-panel">
      <div className="panel-head">
        <div>
          <small>TOP STOCK SETUPS</small>
          <h2>Stock Signals</h2>
        </div>

        <span
          className={
            scanners?.stocks?.running
              ? "state on"
              : "state off"
          }
        >
          {scanners?.stocks?.running
            ? `${scanners?.stocks?.resolved || 0}/40 LIVE`
            : "STOPPED"}
        </span>
      </div>

      {shown.length === 0 ? (
        <div className="empty">
          {scanners?.stocks?.running
            ? "Scanning stocks • waiting for completed candles..."
            : "Start STOCK SIG to scan stocks."}
        </div>
      ) : (
        <div className="stock-list">
          {shown.map((item) => {
            const buy =
              item.direction === "BUY";

            return (
              <div
                className="stock-row"
                key={item.symbol}
              >
                <div>
                  <b>{item.symbol}</b>
                  <small>
                    {item.grade || "—"}
                  </small>
                </div>

                <strong
                  className={
                    buy
                      ? "buy"
                      : "sell"
                  }
                >
                  {item.direction}
                </strong>

                <span>
                  Score{" "}
                  <b>
                    {item.score ?? "—"}
                  </b>
                </span>

                <span>
                  Entry{" "}
                  <b>
                    {item.entry == null
                      ? "—"
                      : `₹${formatNumber(item.entry)}`}
                  </b>
                </span>

                <span>
                  SL{" "}
                  <b>
                    {item.stop_loss == null
                      ? "—"
                      : `₹${formatNumber(item.stop_loss)}`}
                  </b>
                </span>

                <button
                  className="row-order"
                  disabled={!item.actionable}
                  onClick={() =>
                    onOrder({
                      kind: "STOCK",
                      signal: item,
                    })
                  }
                >
                  ORDER
                </button>
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
}

function PositionsPanel({
  positions,
  loading,
  onRefresh,
  onSquareOff,
  onSquareOffAll
}) {
  return (
    <section className="panel positions-panel">
      <div className="panel-head">
        <div>
          <small>ACCOUNT</small>
          <h2>Positions</h2>
        </div>

        <div className="panel-actions">
          <button
            className="icon-btn"
            onClick={onRefresh}
            title="Refresh positions"
          >
            <RefreshCw size={16} />
          </button>

          <button
            className="danger-link"
            disabled={!positions.length}
            onClick={onSquareOffAll}
          >
            SQUARE OFF ALL
          </button>
        </div>
      </div>

      {loading ? (
        <div className="empty">
          Loading positions...
        </div>
      ) : positions.length === 0 ? (
        <div className="empty">
          No open positions.
        </div>
      ) : (
        <div className="position-list">
          {positions.map((p) => (
            <div
              className="position-row"
              key={`${p.exchange_segment}-${p.trading_symbol}`}
            >
              <div>
                <b>
                  {p.trading_symbol}
                </b>
                <small>
                  {p.side} • Qty{" "}
                  {p.net_quantity}
                </small>
              </div>

              <span>
                Avg{" "}
                <b>
                  ₹{formatNumber(
                    p.average_price
                  )}
                </b>
              </span>

              <span>
                LTP{" "}
                <b>
                  {p.ltp == null
                    ? "—"
                    : `₹${formatNumber(p.ltp)}`}
                </b>
              </span>

              <strong
                className={
                  Number(
                    p.unrealized_pnl
                  ) >= 0
                    ? "buy"
                    : "sell"
                }
              >
                {p.unrealized_pnl == null
                  ? "P&L —"
                  : `P&L ₹${formatNumber(
                      p.unrealized_pnl
                    )}`}
              </strong>

              <button
                className="square-btn"
                onClick={() =>
                  onSquareOff(p)
                }
              >
                SQUARE OFF
              </button>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

function ManualOrderModal({
  draft,
  busy,
  onClose,
  onSubmit
}) {
  const [quantity, setQuantity] =
    React.useState(
      String(draft?.quantity || 1)
    );

  const [orderType, setOrderType] =
    React.useState("MKT");

  const [price, setPrice] =
    React.useState("");

  React.useEffect(() => {
    setQuantity(
      String(draft?.quantity || 1)
    );
    setOrderType("MKT");
    setPrice("");
  }, [draft]);

  if (!draft) {
    return null;
  }

  const submit = () => {
    const qty = Number(quantity);

    if (
      !Number.isInteger(qty) ||
      qty <= 0
    ) {
      window.alert(
        "Enter a valid quantity."
      );
      return;
    }

    if (
      orderType === "L" &&
      Number(price) <= 0
    ) {
      window.alert(
        "Enter a valid limit price."
      );
      return;
    }

    const confirmed =
      window.confirm(
        `${draft.side === "B" ? "BUY" : "SELL"} ${qty} ${draft.trading_symbol}?\n\nThis is a REAL manual order.`
      );

    if (!confirmed) {
      return;
    }

    onSubmit({
      ...draft,
      quantity: qty,
      order_type: orderType,
      price:
        orderType === "L"
          ? Number(price)
          : 0,
    });
  };

  return (
    <div className="modal-backdrop">
      <div className="order-modal">
        <button
          className="close-modal"
          onClick={onClose}
        >
          <X size={18} />
        </button>

        <small>MANUAL ONLY</small>
        <h2>Place Order</h2>

        <div className="order-symbol">
          <b>
            {draft.side === "B"
              ? "BUY"
              : "SELL"}
          </b>
          <span>
            {draft.trading_symbol}
          </span>
        </div>

        <div className="order-grid">
          <label>
            Quantity
            <input
              type="number"
              min="1"
              value={quantity}
              onChange={(e) =>
                setQuantity(
                  e.target.value
                )
              }
            />
          </label>

          <label>
            Order Type
            <select
              value={orderType}
              onChange={(e) =>
                setOrderType(
                  e.target.value
                )
              }
            >
              <option value="MKT">
                Market
              </option>
              <option value="L">
                Limit
              </option>
            </select>
          </label>
        </div>

        {orderType === "L" && (
          <label>
            Limit Price
            <input
              type="number"
              step="0.05"
              value={price}
              onChange={(e) =>
                setPrice(
                  e.target.value
                )
              }
            />
          </label>
        )}

        <button
          className="confirm-order"
          disabled={busy}
          onClick={submit}
        >
          {busy
            ? "PLACING..."
            : "CONFIRM REAL ORDER"}
        </button>
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

  const [scannerBusy, setScannerBusy] =
    React.useState("");

  const [signals, setSignals] =
    React.useState({
      "NIFTY 50": null,
      "SENSEX": null,
    });

  const [stockSignals, setStockSignals] =
    React.useState([]);

  const [positions, setPositions] =
    React.useState([]);

  const [positionsLoading, setPositionsLoading] =
    React.useState(false);

  const [orderDraft, setOrderDraft] =
    React.useState(null);

  const [orderBusy, setOrderBusy] =
    React.useState(false);

  const loadScanners =
    React.useCallback(async () => {
      try {
        let response =
          await fetch(
            API +
              "/api/scanners/status"
          );

        if (!response.ok) {
          response =
            await fetch(
              API +
                "/api/scanners"
            );
        }

        if (!response.ok) {
          return;
        }

        setScanners(
          await response.json()
        );
      } catch (_) {}
    }, []);

  const loadSignals =
    React.useCallback(async () => {
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

        const src =
          data?.signals || {};

        setSignals({
          "NIFTY 50":
            src["NIFTY 50"] || null,
          "SENSEX":
            src["SENSEX"] || null,
        });
      } catch (_) {}
    }, []);

  const loadStockSignals =
    React.useCallback(async () => {
      try {
        const response =
          await fetch(
            API +
              "/api/stocks/signals/best?limit=8"
          );

        if (!response.ok) {
          return;
        }

        const data =
          await response.json();

        setStockSignals(
          Array.isArray(data?.items)
            ? data.items
            : []
        );
      } catch (_) {}
    }, []);

  const loadPositions =
    React.useCallback(async () => {
      if (!status.broker_connected) {
        setPositions([]);
        return;
      }

      setPositionsLoading(true);

      try {
        const response =
          await fetch(
            API +
              "/api/positions"
          );

        const data =
          await response.json();

        if (!response.ok) {
          throw new Error(
            data?.detail ||
              "Positions fetch failed."
          );
        }

        setPositions(
          Array.isArray(data?.items)
            ? data.items
            : []
        );
      } catch (error) {
        setMessage(
          `Positions: ${
            error?.message ||
            String(error)
          }`
        );
      } finally {
        setPositionsLoading(false);
      }
    }, [status.broker_connected]);

  React.useEffect(() => {
    loadScanners();
    loadSignals();
    loadStockSignals();

    const timer =
      setInterval(() => {
        loadScanners();
        loadSignals();
        loadStockSignals();
      }, 12000);

    return () =>
      clearInterval(timer);
  }, [
    loadScanners,
    loadSignals,
    loadStockSignals
  ]);

  React.useEffect(() => {
    if (
      !status.broker_connected
    ) {
      return;
    }

    loadPositions();

    const timer =
      setInterval(
        loadPositions,
        15000
      );

    return () =>
      clearInterval(timer);
  }, [
    status.broker_connected,
    loadPositions
  ]);

  React.useEffect(() => {
    let ws = null;
    let reconnectTimer = null;
    let pingTimer = null;
    let destroyed = false;

    function connectSocket() {
      if (destroyed) {
        return;
      }

      try {
        ws =
          new WebSocket(
            WS_URL
          );
      } catch (_) {
        reconnectTimer =
          setTimeout(
            connectSocket,
            3000
          );
        return;
      }

      ws.onopen = () => {
        try {
          ws.send("hello");
        } catch (_) {}

        clearInterval(pingTimer);

        pingTimer =
          setInterval(() => {
            if (
              ws?.readyState ===
              WebSocket.OPEN
            ) {
              try {
                ws.send("ping");
              } catch (_) {}
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
          }

          if (
            msg.type ===
            "snapshot"
          ) {
            const next = {
              ...EMPTY,
            };

            (
              Array.isArray(msg.data)
                ? msg.data
                : []
            ).forEach((item) => {
              if (
                item?.key &&
                next[item.key]
              ) {
                next[item.key] =
                  item;
              }
            });

            setFeed(next);
          }

          if (
            msg.type === "tick"
          ) {
            const item =
              msg.data;

            if (
              item?.key &&
              Object.prototype.hasOwnProperty.call(
                EMPTY,
                item.key
              )
            ) {
              setFeed(
                (prev) => ({
                  ...prev,
                  [item.key]: item,
                })
              );
            }
          }

          if (
            msg.type ===
              "signal_update" ||
            msg.type ===
              "signal_event"
          ) {
            const signal =
              msg.data;

            if (
              signal?.symbol ===
                "NIFTY 50" ||
              signal?.symbol ===
                "SENSEX"
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

          if (
            msg.type ===
            "stock_signal_update"
          ) {
            const signal =
              msg.data;

            if (signal?.symbol) {
              setStockSignals(
                (prev) => {
                  const next = [
                    signal,
                    ...prev.filter(
                      (x) =>
                        x.symbol !==
                        signal.symbol
                    ),
                  ];

                  next.sort(
                    (a, b) =>
                      Number(
                        Boolean(
                          b.actionable
                        )
                      ) -
                        Number(
                          Boolean(
                            a.actionable
                          )
                        ) ||
                      Number(
                        b.score || 0
                      ) -
                        Number(
                          a.score || 0
                        )
                  );

                  return next.slice(
                    0,
                    8
                  );
                }
              );
            }
          }
        } catch (_) {}
      };

      ws.onclose = () => {
        clearInterval(pingTimer);

        if (!destroyed) {
          reconnectTimer =
            setTimeout(
              connectSocket,
              3000
            );
        }
      };
    }

    connectSocket();

    return () => {
      destroyed = true;
      clearInterval(pingTimer);
      clearTimeout(
        reconnectTimer
      );

      try {
        ws?.close();
      } catch (_) {}
    };
  }, []);

  async function connectBroker(
    event
  ) {
    event.preventDefault();

    if (
      !/^\d{6}$/.test(totp)
    ) {
      setLoginError(
        "Enter current 6-digit TOTP."
      );
      return;
    }

    setBusy(true);
    setLoginError("");
    setMessage("Connecting...");

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

      const data =
        await response.json();

      if (!response.ok) {
        const detail =
          data?.detail;

        throw new Error(
          typeof detail ===
            "string"
            ? detail
            : detail?.message ||
                "Connection failed."
        );
      }

      setTotp("");
      setMessage(
        "Live feed connected."
      );
    } catch (error) {
      setTotp("");
      setLoginError(
        error?.message ||
          "Connection failed."
      );
    } finally {
      setBusy(false);
    }
  }

  async function scannerAction(
    path,
    busyName
  ) {
    setScannerBusy(busyName);

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

      setMessage(
        data?.message ||
          "Scanner updated."
      );

      await loadScanners();
      await loadStockSignals();
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

  function openManualOrder({
    kind,
    signal
  }) {
    if (!signal?.actionable) {
      setMessage(
        "Only actionable signals can open the order dialog."
      );
      return;
    }

    if (
      kind ===
      "INDEX_OPTION"
    ) {
      const contract =
        signal.option_contract || {};

      const symbol =
        contract.trading_symbol ||
        contract.display_symbol;

      if (!symbol) {
        setMessage(
          "Option contract is not ready."
        );
        return;
      }

      setOrderDraft({
        exchange_segment:
          contract.exchange_segment ||
          "nse_fo",
        trading_symbol: symbol,
        side: "B",
        product: "MIS",
        quantity:
          Number(
            contract.lot_size || 1
          ),
      });

      return;
    }

    setOrderDraft({
      exchange_segment:
        "nse_cm",
      trading_symbol:
        `${signal.symbol}-EQ`,
      side:
        signal.direction ===
        "SELL"
          ? "S"
          : "B",
      product: "MIS",
      quantity: 1,
    });
  }

  async function submitManualOrder(
    draft
  ) {
    setOrderBusy(true);

    try {
      const response =
        await fetch(
          API +
            "/api/orders/manual",
          {
            method: "POST",
            headers: {
              "Content-Type":
                "application/json",
            },
            body:
              JSON.stringify({
                exchange_segment:
                  draft.exchange_segment,
                trading_symbol:
                  draft.trading_symbol,
                transaction_type:
                  draft.side,
                quantity:
                  draft.quantity,
                product:
                  draft.product,
                order_type:
                  draft.order_type,
                price:
                  draft.price || 0,
                validity:
                  "DAY",
                confirm:
                  true,
                client_request_id:
                  `WEB-${Date.now()}`,
              }),
          }
        );

      const data =
        await response.json();

      if (!response.ok) {
        throw new Error(
          typeof data?.detail ===
            "string"
            ? data.detail
            : JSON.stringify(
                data?.detail ||
                data
              )
        );
      }

      setOrderDraft(null);
      setMessage(
        "Manual order submitted."
      );

      setTimeout(
        loadPositions,
        1200
      );
    } catch (error) {
      setMessage(
        `Order failed: ${
          error?.message ||
          String(error)
        }`
      );
    } finally {
      setOrderBusy(false);
    }
  }

  async function squareOffPosition(
    position
  ) {
    const qty =
      Math.abs(
        Number(
          position.net_quantity
        )
      );

    if (
      !window.confirm(
        `Square off ${qty} ${position.trading_symbol}?\n\nThis is a REAL market order.`
      )
    ) {
      return;
    }

    try {
      const response =
        await fetch(
          API +
            "/api/positions/square-off",
          {
            method: "POST",
            headers: {
              "Content-Type":
                "application/json",
            },
            body:
              JSON.stringify({
                exchange_segment:
                  position.exchange_segment,
                trading_symbol:
                  position.trading_symbol,
                quantity: qty,
                current_net_quantity:
                  Number(
                    position.net_quantity
                  ),
                product:
                  position.product ||
                  "MIS",
                confirm:
                  true,
              }),
          }
        );

      const data =
        await response.json();

      if (!response.ok) {
        throw new Error(
          typeof data?.detail ===
            "string"
            ? data.detail
            : JSON.stringify(
                data?.detail ||
                data
              )
        );
      }

      setMessage(
        `Square-off submitted: ${position.trading_symbol}`
      );

      setTimeout(
        loadPositions,
        1200
      );
    } catch (error) {
      setMessage(
        `Square-off failed: ${
          error?.message ||
          String(error)
        }`
      );
    }
  }

  async function squareOffAll() {
    if (!positions.length) {
      return;
    }

    if (
      !window.confirm(
        `Square off ALL ${positions.length} positions?`
      )
    ) {
      return;
    }

    const typed =
      window.prompt(
        'Type "SQUARE OFF ALL" to confirm.'
      );

    if (
      typed !==
      "SQUARE OFF ALL"
    ) {
      return;
    }

    try {
      const response =
        await fetch(
          API +
            "/api/positions/square-off-all",
          {
            method: "POST",
            headers: {
              "Content-Type":
                "application/json",
            },
            body:
              JSON.stringify({
                confirm_text:
                  typed,
              }),
          }
        );

      const data =
        await response.json();

      if (!response.ok) {
        throw new Error(
          typeof data?.detail ===
            "string"
            ? data.detail
            : JSON.stringify(
                data?.detail ||
                data
              )
        );
      }

      setMessage(
        `Square Off All submitted for ${data.count || 0} positions.`
      );

      setTimeout(
        loadPositions,
        1500
      );
    } catch (error) {
      setMessage(
        `Square Off All failed: ${
          error?.message ||
          String(error)
        }`
      );
    }
  }

  const showLogin =
    !status.feed_connected ||
    Boolean(loginError);

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand-block">
          <div className="crown">
            <Crown size={25} />
          </div>

          <div>
            <h1>
              KING BRO
            </h1>
            <p>
              The RAAJA Bro !!!
            </p>
          </div>
        </div>

        <div className="top-status">
          <span
            className={
              status.feed_connected
                ? "live-pill"
                : "live-pill offline"
            }
          >
            <CircleDot size={13} />
            {status.feed_connected
              ? "LIVE"
              : "OFFLINE"}
          </span>
        </div>
      </header>

      <main className="dashboard">
        <section className="hero-strip">
          {showLogin ? (
            <form
              className="login-inline"
              onSubmit={
                connectBroker
              }
            >
              <input
                value={totp}
                maxLength={6}
                inputMode="numeric"
                placeholder="TOTP"
                onChange={(e) =>
                  setTotp(
                    e.target.value
                      .replace(
                        /\D/g,
                        ""
                      )
                      .slice(0, 6)
                  )
                }
              />

              <button
                disabled={
                  busy ||
                  totp.length !== 6
                }
              >
                {busy
                  ? "CONNECTING"
                  : "CONNECT"}
              </button>

              {loginError && (
                <span className="login-error">
                  {loginError}
                </span>
              )}
            </form>
          ) : (
            <div className="connected-note">
              <Wifi size={17} />
              Live feed connected
            </div>
          )}

          <div className="market-strip">
            <MarketTile
              item={
                feed["NIFTY 50"]
              }
            />
            <MarketTile
              item={
                feed["SENSEX"]
              }
            />
            <MarketTile
              item={
                feed["BANK NIFTY"]
              }
            />
          </div>
        </section>

        <ScannerBar
          scanners={scanners}
          busy={scannerBusy}
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

        <section className="main-grid">
          <div className="panel index-panel">
            <div className="panel-head">
              <div>
                <small>
                  REAL-TIME
                </small>
                <h2>
                  Index Signals
                </h2>
              </div>

              <Zap size={20} />
            </div>

            <div className="index-signal-grid">
              <IndexSignalCard
                title="NIFTY 50 SIGNAL"
                signal={
                  signals[
                    "NIFTY 50"
                  ]
                }
                onOrder={
                  openManualOrder
                }
              />

              <IndexSignalCard
                title="SENSEX SIGNAL"
                signal={
                  signals[
                    "SENSEX"
                  ]
                }
                onOrder={
                  openManualOrder
                }
              />
            </div>
          </div>

          <div className="right-column">
            <StockSignals
              items={stockSignals}
              scanners={scanners}
              onOrder={
                openManualOrder
              }
            />

            <PositionsPanel
              positions={positions}
              loading={
                positionsLoading
              }
              onRefresh={
                loadPositions
              }
              onSquareOff={
                squareOffPosition
              }
              onSquareOffAll={
                squareOffAll
              }
            />
          </div>
        </section>

        {message && (
          <div className="toast-line">
            {message}
          </div>
        )}
      </main>

      <ManualOrderModal
        draft={orderDraft}
        busy={orderBusy}
        onClose={() =>
          setOrderDraft(null)
        }
        onSubmit={
          submitManualOrder
        }
      />
    </div>
  );
}

createRoot(
  document.getElementById(
    "root"
  )
).render(<App />);
