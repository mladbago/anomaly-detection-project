# dashboard/app.py
"""Real-time fraud-detection dashboard (Streamlit).

A single-page, auto-refreshing view over the whole pipeline:

  * live metrics  -- normal vs. anomalous transaction counters,
  * live map      -- where charges (blue) and GPS-jump alerts (red) land,
  * live charts   -- transaction/alert volume over time + anomaly breakdown,
  * live feeds    -- the most recent transactions and fraud alerts.

All heavy lifting (the Kafka tap) is in :mod:`dashboard.stream_state`; this file
is pure presentation. The live panels live in an ``st.fragment`` that reruns on
its own timer, so the page updates without a full-script reload.
"""
import os
import sys

import pandas as pd
import streamlit as st

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import config
from dashboard.stream_state import StreamState

st.set_page_config(page_title="Fraud Detection — Live", page_icon="💳", layout="wide")

# Brand colours for the two map layers.
COLOR_TX = "#1f77b4"        # normal transaction (blue)
COLOR_JUMP = "#d62728"      # gps_jump / impossible-travel alert (red)


@st.cache_resource
def get_state() -> StreamState:
    """One StreamState per server process; starts the Kafka tap on first call."""
    state = StreamState()
    state.start()
    return state


state = get_state()

# --- sidebar ----------------------------------------------------------------
st.sidebar.title("💳 Fraud Detection")
st.sidebar.caption("Real-time payment-card anomaly monitoring")
refresh = st.sidebar.slider("Refresh interval (seconds)", 1, 10, 2)
st.sidebar.markdown(
    f"""
**Pipeline**
- Kafka transactions: `{config.TOPIC_TRANSACTIONS}`
- Kafka alerts: `{config.TOPIC_ALERTS}`
- Mongo: `{config.MONGO_DB_NAME}.{config.MONGO_COLLECTION_ALERTS}`
"""
)
with st.sidebar.expander("Persisted alerts (MongoDB)", expanded=False):
    persisted = StreamState.persisted_alert_counts()
    if persisted:
        st.dataframe(pd.DataFrame(persisted), hide_index=True, use_container_width=True)
    else:
        st.info("No persisted alerts yet (or MongoDB unreachable).")


@st.fragment(run_every=f"{refresh}s")
def live_view():
    """Everything that updates on the refresh timer."""
    snap = state.snapshot()

    status = "🟢 connected" if snap["connected"] else "🔴 connecting…"
    st.markdown(f"### Live stream &nbsp;·&nbsp; {status}")
    if not snap["connected"] and snap["last_error"]:
        st.warning(f"Kafka: {snap['last_error']}")

    # --- metric counters: anomalies caught vs. normal traffic ---------------
    total = snap["total_tx"]
    alerts = snap["total_alerts"]
    normal = max(total - snap["injected_tx"], 0)
    rate = (alerts / total * 100) if total else 0.0
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Transactions", f"{total:,}")
    c2.metric("Normal (approx)", f"{normal:,}")
    c3.metric("Injected anomalies", f"{snap['injected_tx']:,}")
    c4.metric("Alerts caught", f"{alerts:,}")
    c5.metric("Alert rate", f"{rate:.1f}%")

    # --- live map -----------------------------------------------------------
    st.markdown("#### Transaction & GPS-jump map")
    points = snap["geo_points"]
    if points:
        df = pd.DataFrame(points)
        df["color"] = df["kind"].map({"transaction": COLOR_TX, "gps_jump": COLOR_JUMP})
        df["size"] = df["kind"].map({"transaction": 40, "gps_jump": 260})
        st.map(df, latitude="lat", longitude="lon", color="color", size="size")
        st.caption("🔵 transactions  ·  🔴 impossible-travel (gps_jump) alerts")
    else:
        st.info("Waiting for transactions to plot…")

    # --- charts -------------------------------------------------------------
    left, right = st.columns(2)
    with left:
        st.markdown("#### Volume over time (per second)")
        series = pd.DataFrame(snap["series"])
        series["t"] = pd.to_datetime(series["time"], unit="s")
        st.line_chart(series.set_index("t")[["transactions", "alerts"]], height=260)
    with right:
        st.markdown("#### Anomaly breakdown by type")
        counts = snap["alert_counts"]
        if counts:
            breakdown = pd.DataFrame(
                sorted(counts.items(), key=lambda kv: -kv[1]), columns=["alert_type", "count"]
            ).set_index("alert_type")
            st.bar_chart(breakdown, height=260)
        else:
            st.info("No alerts yet.")

    # --- live feeds ---------------------------------------------------------
    feed_l, feed_r = st.columns(2)
    with feed_l:
        st.markdown("#### Recent transactions")
        st.dataframe(_tx_table(snap["recent_tx"]), hide_index=True, use_container_width=True)
    with feed_r:
        st.markdown("#### Recent alerts")
        st.dataframe(_alert_table(snap["recent_alerts"]), hide_index=True, use_container_width=True)


def _tx_table(rows) -> pd.DataFrame:
    """Flatten recent transactions into a display table."""
    return pd.DataFrame([
        {
            "card": r.get("card_id"),
            "value": round(r.get("transaction_value", 0), 2),
            "lat": r.get("location", {}).get("lat"),
            "lon": r.get("location", {}).get("lon"),
            "injected": r.get("is_injected_anomaly", False),
            "expected": r.get("expected_anomaly") or "",
        }
        for r in rows
    ])


def _alert_table(rows) -> pd.DataFrame:
    """Flatten recent alerts into a display table."""
    return pd.DataFrame([
        {
            "alert": r.get("alert_type"),
            "card": r.get("card_id"),
            "value": round(r.get("transaction", {}).get("transaction_value", 0), 2),
            "injected_truth": r.get("transaction", {}).get("is_injected_anomaly", False),
        }
        for r in rows
    ])


live_view()
