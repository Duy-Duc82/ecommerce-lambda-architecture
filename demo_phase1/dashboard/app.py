"""
Dashboard chinh — Streamlit App.

Trang tong quan cho he thong Big Data TMDT.

Chay:
  streamlit run dashboard/app.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ── Page config ──────────────────────────────────────────────
st.set_page_config(
    page_title="E-Commerce Big Data Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ───────────────────────────────────────────────
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #888;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border-radius: 12px;
        padding: 1.5rem;
        border: 1px solid #2a2a4a;
    }
    .stMetric > div {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border-radius: 12px;
        padding: 1rem;
        border: 1px solid #2a2a4a;
    }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ──────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/combo-chart.png", width=64)
    st.title("Lambda Platform")
    st.markdown("---")

    page = st.radio(
        "📌 Navigation",
        [
            "🏠 Overview",
            "📈 Trends",
            "🚨 Anomalies",
            "⚡ Real-time",
            "🔍 Fraud Detection",
        ],
        index=0,
    )

    st.markdown("---")
    st.markdown("### 🔗 Quick Links")
    st.markdown("- [Spark UI](http://localhost:8080)")
    st.markdown("- [MinIO Console](http://localhost:9001)")
    st.markdown("- [Kibana](http://localhost:5601)")

    st.markdown("---")
    st.caption("Lambda Architecture - TMDT Big Data")

# ── Main Content ─────────────────────────────────────────────

if page == "🏠 Overview":
    st.markdown('<p class="main-header">📊 E-Commerce Analytics Overview</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Nen tang phan tich du lieu lon cho he thong Thuong Mai Dien Tu</p>', unsafe_allow_html=True)

    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)

    try:
        from serving_layer.redis_cache import get_active_users, get_fraud_alerts, get_realtime_event_counts, get_realtime_revenue

        counts = get_realtime_event_counts()
        revenue_data = get_realtime_revenue(1)
        alerts = get_fraud_alerts(100)
        active = get_active_users()

        total_events = sum(counts.values())
        total_revenue = revenue_data[0]["total_revenue"] if revenue_data else 0

        col1.metric("📦 Tổng Events", f"{total_events:,}")
        col2.metric("💰 Doanh thu (5m)", f"{total_revenue:,.0f} ₫")
        col3.metric("👥 Users Active", f"{active:,}")
        col4.metric("🚨 Fraud Alerts", f"{len(alerts)}")

    except Exception:
        col1.metric("📦 Tổng Events", "—")
        col2.metric("💰 Doanh thu", "—")
        col3.metric("👥 Users Active", "—")
        col4.metric("🚨 Fraud Alerts", "—")
        st.warning("⚠️ Không kết nối được Redis. Hãy chạy `docker compose up -d` trước.")

    # Architecture Diagram
    st.markdown("### 🏗️ Kiến trúc Lambda")
    st.code("""
    ┌─────────────┐     ┌────────┐     ┌──────────────────────┐
    │  Producers   │────▶│ Kafka  │────▶│     SPEED LAYER      │
    │ (Simulators) │     │(Topics)│     │ Spark Streaming→Redis│
    └─────────────┘     └───┬────┘     └──────────────────────┘
                            │
                            ▼
                    ┌──────────────┐     ┌──────────────────┐
                    │  MinIO/HDFS  │     │   SERVING LAYER  │
                    │ (Data Lake)  │     │  Postgres + Redis│
                    └──────┬───────┘     └────────┬─────────┘
                           │                      │
                           ▼                      ▼
                    ┌──────────────┐     ┌──────────────────┐
                    │ BATCH LAYER  │────▶│   DASHBOARD      │
                    │ (ETL + ML)   │     │  (Streamlit)     │
                    └──────────────┘     └──────────────────┘
    """, language=None)

    # System Status
    st.markdown("### 📡 System Status")
    import socket

    services = {
        "Kafka (9092)": ("localhost", 9092),
        "Spark Master (8080)": ("localhost", 8080),
        "MinIO (9000)": ("localhost", 9000),
        "Redis (6379)": ("localhost", 6379),
        "Postgres (5432)": ("localhost", 5432),
    }

    cols = st.columns(len(services))
    for col, (name, (host, port)) in zip(cols, services.items()):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect((host, port))
            s.close()
            col.success(f"✅ {name}")
        except (socket.error, socket.timeout):
            col.error(f"❌ {name}")


elif page == "📈 Trends":
    st.markdown('<p class="main-header">📈 Phân tích Xu hướng</p>', unsafe_allow_html=True)

    try:
        import psycopg2
        import pandas as pd
        import plotly.express as px
        from config.settings import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB

        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            user=POSTGRES_USER, password=POSTGRES_PASSWORD, dbname=POSTGRES_DB,
        )

        # Trending Products
        st.subheader("🔥 Top Trending Products")
        try:
            trending_df = pd.read_sql("SELECT * FROM ml_trending_products ORDER BY trend_score DESC LIMIT 20", conn)
            if not trending_df.empty:
                fig = px.bar(
                    trending_df, x="product_name", y="trend_score",
                    color="category", title="Trending Products by Score",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig.update_layout(template="plotly_dark", xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Chưa có dữ liệu trending. Hãy chạy batch ETL trước.")
        except Exception as e:
            st.info(f"Chạy trend analysis trước: `python -m batch_layer.models.trend_analysis`")

        # Category Performance
        st.subheader("📊 Category Performance")
        try:
            cat_df = pd.read_sql("SELECT * FROM agg_product_stats ORDER BY purchases DESC LIMIT 20", conn)
            if not cat_df.empty:
                fig = px.scatter(
                    cat_df, x="views", y="purchases", size="conversion_rate",
                    color="category", hover_name="product_name",
                    title="Views vs Purchases (kích thước = Conversion Rate)",
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                )
                fig.update_layout(template="plotly_dark")
                st.plotly_chart(fig, use_container_width=True)
        except Exception:
            st.info("Chạy batch ETL trước để có dữ liệu.")

        conn.close()

    except Exception as e:
        st.error(f"Không kết nối được Postgres: {e}")


elif page == "🚨 Anomalies":
    st.markdown('<p class="main-header">🚨 Phát hiện Bất thường</p>', unsafe_allow_html=True)

    try:
        import psycopg2
        import pandas as pd
        from config.settings import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB

        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            user=POSTGRES_USER, password=POSTGRES_PASSWORD, dbname=POSTGRES_DB,
        )

        # Z-Score Anomalies
        st.subheader("📉 Z-Score Anomalies")
        try:
            anomaly_df = pd.read_sql("SELECT * FROM ml_anomaly_zscore ORDER BY event_time DESC LIMIT 50", conn)
            if not anomaly_df.empty:
                # Severity summary
                cols = st.columns(3)
                for i, sev in enumerate(["CRITICAL", "HIGH", "MEDIUM"]):
                    count = len(anomaly_df[anomaly_df["severity"] == sev])
                    color = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡"}[sev]
                    cols[i].metric(f"{color} {sev}", count)

                st.dataframe(anomaly_df, use_container_width=True)
            else:
                st.info("Không tìm thấy anomaly.")
        except Exception:
            st.info("Chạy anomaly detection: `python -m batch_layer.models.anomaly_detection`")

        # Spike/Drop
        st.subheader("📊 Spike / Drop Detection")
        try:
            spike_df = pd.read_sql("SELECT * FROM ml_anomaly_spikes", conn)
            if not spike_df.empty:
                st.dataframe(spike_df, use_container_width=True)
        except Exception:
            st.info("Chưa có dữ liệu spike/drop.")

        conn.close()

    except Exception as e:
        st.error(f"Lỗi: {e}")


elif page == "⚡ Real-time":
    st.markdown('<p class="main-header">⚡ Real-time Metrics</p>', unsafe_allow_html=True)

    auto_refresh = st.toggle("🔄 Auto Refresh (5s)", value=False)
    if auto_refresh:
        import time
        st.empty()

    try:
        from serving_layer.redis_cache import (
            get_cache_stats,
            get_fraud_alerts,
            get_realtime_event_counts,
            get_realtime_revenue,
            get_top_products,
        )

        # Cache Stats
        stats = get_cache_stats()
        c1, c2, c3 = st.columns(3)
        c1.metric("🔑 Total Keys", stats["total_keys"])
        c2.metric("⚡ Realtime Keys", stats["realtime_keys"])
        c3.metric("💾 Memory Used", stats["used_memory"])

        # Revenue Timeline
        st.subheader("💰 Revenue (5-min windows)")
        revenue = get_realtime_revenue(24)
        if revenue:
            import pandas as pd
            import plotly.express as px

            rev_df = pd.DataFrame(revenue)
            fig = px.bar(
                rev_df, x="window_start", y="total_revenue",
                title="Revenue per 5-minute window",
                color_discrete_sequence=["#667eea"],
            )
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Chưa có dữ liệu revenue real-time.")

        # Event Counts
        st.subheader("📊 Real-time Event Counts")
        counts = get_realtime_event_counts()
        if counts:
            import pandas as pd

            counts_df = pd.DataFrame([
                {"metric": k, "count": v} for k, v in sorted(counts.items(), key=lambda x: -x[1])[:20]
            ])
            st.dataframe(counts_df, use_container_width=True)
        else:
            st.info("Chưa có events. Hãy chạy producer và speed layer.")

        # Alerts
        st.subheader("🚨 Recent Fraud Alerts")
        alerts = get_fraud_alerts(10)
        if alerts:
            for alert in alerts:
                st.warning(
                    f"**{alert.get('alert_type', 'ALERT')}** — "
                    f"User: `{alert.get('user_id', '?')}` | "
                    f"Amount: {alert.get('total_amount', 0):,.0f} ₫ | "
                    f"Time: {alert.get('event_time', '?')}"
                )
        else:
            st.success("✅ Không có cảnh báo gian lận.")

    except Exception as e:
        st.error(f"Không kết nối được Redis: {e}")
        st.info("Hãy chạy `docker compose up -d` và `python -m data_ingestion.producer`")

    if auto_refresh:
        time.sleep(5)
        st.rerun()


elif page == "🔍 Fraud Detection":
    st.markdown('<p class="main-header">🔍 Phát hiện Gian lận</p>', unsafe_allow_html=True)

    try:
        import psycopg2
        import pandas as pd
        import plotly.express as px
        from config.settings import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB

        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            user=POSTGRES_USER, password=POSTGRES_PASSWORD, dbname=POSTGRES_DB,
        )

        # High Value Orders
        st.subheader("💸 High Value Suspicious Orders")
        try:
            hv_df = pd.read_sql("SELECT * FROM ml_fraud_high_value ORDER BY risk_score DESC LIMIT 20", conn)
            if not hv_df.empty:
                st.dataframe(hv_df, use_container_width=True)
        except Exception:
            st.info("Chạy fraud detection: `python -m batch_layer.models.fraud_detection`")

        # Isolation Forest Results
        st.subheader("🤖 ML Fraud Detection (Isolation Forest)")
        try:
            if_df = pd.read_sql(
                "SELECT * FROM ml_fraud_isolation_forest WHERE is_fraud = true ORDER BY anomaly_score DESC LIMIT 20",
                conn,
            )
            if not if_df.empty:
                st.metric("🚩 Flagged Users", len(if_df))

                fig = px.scatter(
                    if_df, x="total_spent", y="purchase_count",
                    size="anomaly_score", color="is_fraud",
                    hover_data=["user_id", "avg_order_value"],
                    title="Fraud Detection — Total Spent vs Purchase Count",
                    color_discrete_sequence=["#ff6b6b", "#51cf66"],
                )
                fig.update_layout(template="plotly_dark")
                st.plotly_chart(fig, use_container_width=True)

                st.dataframe(if_df[["user_id", "total_spent", "purchase_count", "avg_order_value", "anomaly_score"]], use_container_width=True)
        except Exception:
            st.info("Chạy fraud detection trước.")

        # User Features
        st.subheader("👤 User Feature Analysis")
        try:
            feat_df = pd.read_sql("SELECT * FROM ml_user_features ORDER BY total_spent DESC LIMIT 20", conn)
            if not feat_df.empty:
                fig = px.scatter(
                    feat_df, x="view_count", y="purchase_count",
                    size="total_spent", color="purchase_rate",
                    hover_data=["user_id"],
                    title="User Behavior — Views vs Purchases",
                    color_continuous_scale="Viridis",
                )
                fig.update_layout(template="plotly_dark")
                st.plotly_chart(fig, use_container_width=True)
        except Exception:
            pass

        conn.close()

    except Exception as e:
        st.error(f"Lỗi: {e}")
