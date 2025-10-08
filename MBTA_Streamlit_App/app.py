import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
from datetime import datetime

# =====================================================
# 🌐 PAGE CONFIGURATION
# =====================================================
st.set_page_config(
    page_title="MBTA Live Predictions Dashboard",
    page_icon="🚆",
    layout="wide"
)

# =====================================================
# 🧭 HEADER SECTION
# =====================================================
st.markdown(
    """
    <style>
        .main-title {
            font-size: 42px !important;
            font-weight: 700 !important;
            color: #1E88E5;
            letter-spacing: 0.5px;
        }
        .subtitle {
            font-size: 18px;
            color: #aaaaaa;
        }
        .stMetric label {font-size:16px !important;}
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown("<h1 class='main-title'>🚆 MBTA Live Predictions Dashboard</h1>", unsafe_allow_html=True)
st.markdown("<p class='subtitle'>Real-time MBTA Green-B Line data — extracted using Apache Airflow, validated, stored in Snowflake, and visualized via Streamlit.</p>", unsafe_allow_html=True)

# =====================================================
# ❄️ SNOWFLAKE CONNECTION
# =====================================================
def get_snowflake_connection():
    return snowflake.connector.connect(
        user="RITHIKA0311",
        password="Charliembta@12345",
        account="vrc94697.us-east-1",
        warehouse="COMPUTE_WH",
        database="SNOWFLAKE_LEARNING_DB",
        schema="PUBLIC"
    )

# =====================================================
# 📦 LOAD DATA FROM SNOWFLAKE (Cached for 2 mins)
# =====================================================
@st.cache_data(ttl=120)
def load_mbta_data():
    conn = get_snowflake_connection()
    query = """
        SELECT * 
        FROM MBTA_LIVE_PREDICTIONS
        ORDER BY LOAD_TIMESTAMP DESC
        LIMIT 200;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# =====================================================
# 🧩 FETCH DATA
# =====================================================
df = load_mbta_data()

if df.empty:
    st.warning("⚠️ No MBTA data found in Snowflake. Please ensure your Airflow DAG has run successfully.")
    st.stop()

# Convert timestamps
df["LOAD_TIMESTAMP"] = pd.to_datetime(df["LOAD_TIMESTAMP"])
df["ARRIVAL_TIME"] = pd.to_datetime(df["ARRIVAL_TIME"], errors="coerce")
df["DEPARTURE_TIME"] = pd.to_datetime(df["DEPARTURE_TIME"], errors="coerce")

st.success(f"✅ {len(df)} records loaded from Snowflake at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# =====================================================
# ✨ SYSTEM OVERVIEW SECTION
# =====================================================
st.markdown("### 🚦 System Overview")
st.markdown(
    """
    This section summarizes the overall data health — showing total records, latest load time, and data freshness.  
    It ensures that the ETL pipeline is running correctly and updating the data in Snowflake on schedule.
    """
)

# ---- KPIs ----
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("🛤 Total Records", f"{len(df):,}")
with col2:
    last_time = df["LOAD_TIMESTAMP"].max().strftime("%Y-%m-%d %H:%M:%S")
    st.metric("⏱ Last Data Load", last_time)
with col3:
    # Always compute freshness dynamically (not cached)
    latest_timestamp = pd.to_datetime(df["LOAD_TIMESTAMP"].max())
    freshness_minutes = int((datetime.utcnow() - latest_timestamp.to_pydatetime()).total_seconds() // 60)
    freshness_text = "Fresh ✅" if freshness_minutes < 10 else "Stale ⚠️"
    st.metric("🕒 Data Freshness", f"{freshness_minutes} min ago", freshness_text)

st.markdown("---")

# =====================================================
# 📊 DIRECTION COMPARISON
# =====================================================
st.markdown("### 🚉 Direction Comparison (Inbound vs Outbound)")
st.markdown(
    """
    This visualization compares the number of trains traveling **inbound (toward Boston)** versus **outbound (away from Boston)**.  
    - **X-axis:** Represents the count of train predictions recorded in each direction.  
    - **Y-axis:** Distinguishes direction type — Inbound (0) or Outbound (1).  
    This helps us see which way has more active train traffic in real time.
    """
)

dir_labels = {0: "Inbound (0)", 1: "Outbound (1)"}
direction_counts = df["DIRECTION"].map(dir_labels).value_counts().reset_index()
direction_counts.columns = ["Direction", "Trains"]

fig_dir = px.bar(
    direction_counts,
    x="Trains",
    y="Direction",
    orientation="h",
    text="Trains",
    color="Direction",
    color_discrete_sequence=["#4A90E2", "#50E3C2"],
    title="Train Volume by Direction",
    template="plotly_dark"
)
fig_dir.update_traces(textposition="outside", textfont_size=14)
fig_dir.update_layout(showlegend=False, xaxis_title="Number of Trains", yaxis_title=None)
st.plotly_chart(fig_dir, use_container_width=True)

# =====================================================
# 📈 ARRIVAL TREND OVER TIME
# =====================================================
st.markdown("### ⏰ Arrival Trend (Last Few Hours)")
st.markdown(
    """
    This chart shows how train arrivals are distributed over time, helping identify frequency and movement patterns.  
    - **X-axis:** Arrival timestamps for MBTA Green-B trains.  
    - **Y-axis:** Stop sequence identifiers (representing different stations).  
    A stable pattern indicates consistent arrivals, while spikes or dips may suggest schedule irregularities or delays.
    """
)

df_sorted = df.sort_values("ARRIVAL_TIME")
fig_trend = px.line(
    df_sorted,
    x="ARRIVAL_TIME",
    y="STOP_SEQ",
    markers=True,
    line_shape="spline",
    color_discrete_sequence=["#1E88E5"],
    title="Train Arrival Pattern by Stop Sequence",
    template="plotly_dark"
)
fig_trend.update_xaxes(title="Arrival Time", showgrid=True)
fig_trend.update_yaxes(title="Stop Sequence")
st.plotly_chart(fig_trend, use_container_width=True)

# =====================================================
# 🧾 RAW DATA TABLE
# =====================================================
st.markdown("### 📋 Latest Data Records")
st.markdown(
    """
    Below is the latest batch of real-time MBTA Green-B predictions fetched from the API and stored in Snowflake.  
    You can use this table to verify individual records or cross-check the most recent updates.
    """
)

with st.expander("🔍 View Latest Records"):
    st.dataframe(df, use_container_width=True)

# =====================================================
# 🔁 AUTO-REFRESH + BRAND FOOTER
# =====================================================

# Auto-refresh every 5 minutes (300000 ms)
st.markdown(
    """
    <script>
        setTimeout(() => { window.location.reload(); }, 300000);
    </script>
    """,
    unsafe_allow_html=True
)

# Subtle professional footer
st.markdown("---")
st.markdown(
    """
    <div style="text-align:center; color:gray; font-size:15px; margin-top:15px;">
        🚀 Built with ❤️ using <b>Apache Airflow</b>, <b>Snowflake</b>, and <b>Streamlit</b><br>
        <span style="color:#1E88E5;">Rithika Sankar Rajeswari</span> | Data Analytics Engineering @ Northeastern University
    </div>
    """,
    unsafe_allow_html=True
)
