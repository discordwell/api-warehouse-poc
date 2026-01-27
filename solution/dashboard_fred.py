#!/usr/bin/env python3
"""
FRED Economic Dashboard

Visualizes Federal Reserve economic indicators.
Run with: streamlit run dashboard_fred.py
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime, timedelta

st.set_page_config(
    page_title="FRED Economic Dashboard",
    page_icon="🏦",
    layout="wide"
)

DB_PATH = Path(__file__).parent / "warehouse.duckdb"


@st.cache_resource
def get_connection():
    return duckdb.connect(str(DB_PATH), read_only=True)


def load_series():
    conn = get_connection()
    return conn.execute("SELECT * FROM fred_series").fetchdf()


def load_observations(series_id: str, years: int = 10):
    conn = get_connection()
    cutoff = datetime.now() - timedelta(days=years * 365)
    return conn.execute("""
        SELECT observation_date, value
        FROM fred_observations
        WHERE series_id = ?
        AND observation_date >= ?
        ORDER BY observation_date
    """, [series_id, cutoff.strftime('%Y-%m-%d')]).fetchdf()


def load_all_series_data(years: int = 10):
    conn = get_connection()
    cutoff = datetime.now() - timedelta(days=years * 365)
    return conn.execute("""
        SELECT o.series_id, o.observation_date, o.value, s.title, s.units
        FROM fred_observations o
        JOIN fred_series s ON o.series_id = s.series_id
        WHERE o.observation_date >= ?
        ORDER BY o.observation_date
    """, [cutoff.strftime('%Y-%m-%d')]).fetchdf()


# Header
st.title("🏦 FRED Economic Dashboard")
st.caption("Federal Reserve Economic Data - GDP, Unemployment, Inflation, Interest Rates")

series_df = load_series()

if series_df.empty:
    st.error("No FRED data found. Run `python ingest_fred.py` first.")
    st.stop()

# Sidebar controls
st.sidebar.header("Settings")
years = st.sidebar.slider("Years of history", 1, 75, 10)

# Latest values metrics
st.header("Current Indicators")
col1, col2, col3, col4 = st.columns(4)

metrics = {
    'GDP': ('💵', 'Billions USD', col1),
    'UNRATE': ('👷', '%', col2),
    'CPIAUCSL': ('🛒', 'Index', col3),
    'FEDFUNDS': ('🏛️', '%', col4),
}

for series_id, (icon, unit, col) in metrics.items():
    obs = load_observations(series_id, years=2)
    if not obs.empty:
        latest = obs.iloc[-1]['value']
        prev = obs.iloc[-2]['value'] if len(obs) > 1 else latest
        delta = latest - prev
        pct_change = (delta / prev * 100) if prev != 0 else 0

        series_info = series_df[series_df['series_id'] == series_id].iloc[0]

        with col:
            st.metric(
                label=f"{icon} {series_id}",
                value=f"{latest:,.2f}",
                delta=f"{pct_change:+.1f}%",
                help=series_info['title']
            )

st.divider()

# Main charts
st.header("Historical Trends")

tab1, tab2, tab3, tab4 = st.tabs(["GDP", "Unemployment", "Inflation (CPI)", "Fed Funds Rate"])

with tab1:
    gdp_df = load_observations('GDP', years)
    if not gdp_df.empty:
        fig = px.area(gdp_df, x='observation_date', y='value',
                     title="Gross Domestic Product (Quarterly)",
                     labels={'observation_date': '', 'value': 'Billions USD'})
        fig.update_layout(height=450)
        fig.update_traces(fill='tozeroy', line_color='#2ecc71')
        st.plotly_chart(fig, use_container_width=True)

        # YoY growth
        gdp_df['yoy_growth'] = gdp_df['value'].pct_change(4) * 100
        fig2 = px.bar(gdp_df.dropna(), x='observation_date', y='yoy_growth',
                     title="Year-over-Year GDP Growth",
                     labels={'observation_date': '', 'yoy_growth': '% Change'})
        fig2.update_traces(marker_color=gdp_df.dropna()['yoy_growth'].apply(
            lambda x: '#2ecc71' if x >= 0 else '#e74c3c'))
        fig2.update_layout(height=300)
        st.plotly_chart(fig2, use_container_width=True)

with tab2:
    unrate_df = load_observations('UNRATE', years)
    if not unrate_df.empty:
        fig = px.line(unrate_df, x='observation_date', y='value',
                     title="Unemployment Rate (Monthly)",
                     labels={'observation_date': '', 'value': 'Percent'})
        fig.update_traces(line_color='#e74c3c', line_width=2)
        fig.update_layout(height=450)

        # Add recession bands would go here if we had recession data
        avg_rate = unrate_df['value'].mean()
        fig.add_hline(y=avg_rate, line_dash="dash", line_color="gray",
                     annotation_text=f"Avg: {avg_rate:.1f}%")
        st.plotly_chart(fig, use_container_width=True)

        col1, col2, col3 = st.columns(3)
        col1.metric("Current", f"{unrate_df.iloc[-1]['value']:.1f}%")
        col2.metric("Period High", f"{unrate_df['value'].max():.1f}%")
        col3.metric("Period Low", f"{unrate_df['value'].min():.1f}%")

with tab3:
    cpi_df = load_observations('CPIAUCSL', years)
    if not cpi_df.empty:
        # CPI level
        fig = px.line(cpi_df, x='observation_date', y='value',
                     title="Consumer Price Index (Monthly)",
                     labels={'observation_date': '', 'value': 'Index (1982-84=100)'})
        fig.update_traces(line_color='#9b59b6', line_width=2)
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

        # Inflation rate (YoY % change)
        cpi_df['inflation'] = cpi_df['value'].pct_change(12) * 100
        fig2 = px.area(cpi_df.dropna(), x='observation_date', y='inflation',
                      title="Inflation Rate (Year-over-Year % Change)",
                      labels={'observation_date': '', 'inflation': '% Change'})
        fig2.update_traces(fill='tozeroy', line_color='#9b59b6')
        fig2.add_hline(y=2, line_dash="dash", line_color="green",
                      annotation_text="2% Target")
        fig2.update_layout(height=350)
        st.plotly_chart(fig2, use_container_width=True)

with tab4:
    ff_df = load_observations('FEDFUNDS', years)
    if not ff_df.empty:
        fig = px.line(ff_df, x='observation_date', y='value',
                     title="Federal Funds Effective Rate (Monthly)",
                     labels={'observation_date': '', 'value': 'Percent'})
        fig.update_traces(line_color='#3498db', line_width=2)
        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)

        # Rate change analysis
        ff_df['change'] = ff_df['value'].diff()
        recent_changes = ff_df.tail(12)

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Last 12 Months")
            direction = "↑ Hiking" if recent_changes['change'].sum() > 0 else "↓ Cutting"
            st.metric("Trend", direction, f"{recent_changes['change'].sum():+.2f}%")
        with col2:
            st.subheader("Current Rate")
            st.metric("Fed Funds", f"{ff_df.iloc[-1]['value']:.2f}%")

# Comparison view
st.divider()
st.header("Multi-Series Comparison")

all_data = load_all_series_data(years)
if not all_data.empty:
    # Normalize to 100 at start for comparison
    selected = st.multiselect(
        "Select series to compare (normalized to 100)",
        options=['GDP', 'UNRATE', 'CPIAUCSL', 'FEDFUNDS'],
        default=['GDP', 'UNRATE']
    )

    if selected:
        fig = go.Figure()
        for sid in selected:
            sdata = all_data[all_data['series_id'] == sid].copy()
            if not sdata.empty:
                sdata['normalized'] = (sdata['value'] / sdata['value'].iloc[0]) * 100
                fig.add_trace(go.Scatter(
                    x=sdata['observation_date'],
                    y=sdata['normalized'],
                    name=sid,
                    mode='lines'
                ))

        fig.update_layout(
            title="Normalized Comparison (Start = 100)",
            height=400,
            yaxis_title="Index (Start = 100)",
            xaxis_title=""
        )
        st.plotly_chart(fig, use_container_width=True)

# Footer
st.divider()
st.caption(f"Data source: Federal Reserve Bank of St. Louis (FRED) | Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
