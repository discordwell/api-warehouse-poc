#!/usr/bin/env python3
"""
API Warehouse Dashboard

Visualizes FRED economic data and Hacker News trends.
Run with: streamlit run dashboard.py
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
from datetime import datetime, timedelta

# Config
st.set_page_config(
    page_title="API Warehouse Dashboard",
    page_icon="📊",
    layout="wide"
)

DB_PATH = Path(__file__).parent / "warehouse.duckdb"


@st.cache_resource
def get_connection():
    return duckdb.connect(str(DB_PATH), read_only=True)


def load_fred_series():
    conn = get_connection()
    return conn.execute("SELECT * FROM fred_series").fetchdf()


def load_fred_observations(series_id: str, years: int = 10):
    conn = get_connection()
    cutoff = datetime.now() - timedelta(days=years * 365)
    return conn.execute("""
        SELECT observation_date, value
        FROM fred_observations
        WHERE series_id = ?
        AND observation_date >= ?
        ORDER BY observation_date
    """, [series_id, cutoff.strftime('%Y-%m-%d')]).fetchdf()


def load_hn_stories():
    conn = get_connection()
    return conn.execute("""
        SELECT id, title, score, descendants, "by", time,
               to_timestamp(time) as posted_at
        FROM hn_items
        WHERE type = 'story'
        ORDER BY score DESC
    """).fetchdf()


def load_hn_stats():
    conn = get_connection()
    return conn.execute("""
        SELECT
            type,
            COUNT(*) as count,
            AVG(score) as avg_score
        FROM hn_items
        GROUP BY type
    """).fetchdf()


def load_hn_top_authors():
    conn = get_connection()
    return conn.execute("""
        SELECT "by" as author, COUNT(*) as items, SUM(score) as total_score
        FROM hn_items
        WHERE "by" IS NOT NULL
        GROUP BY "by"
        ORDER BY total_score DESC
        LIMIT 10
    """).fetchdf()


# Header
st.title("📊 API Warehouse Dashboard")
st.markdown("*Economic indicators from FRED + Tech news from Hacker News*")

# Tabs
tab1, tab2, tab3 = st.tabs(["🏦 Economic Indicators", "📰 Hacker News", "🔍 Data Explorer"])

# ===========================================
# TAB 1: FRED Economic Data
# ===========================================
with tab1:
    st.header("Federal Reserve Economic Data")

    series_df = load_fred_series()

    if series_df.empty:
        st.warning("No FRED data found. Run ingest_fred.py first.")
    else:
        # Metrics row
        col1, col2, col3, col4 = st.columns(4)

        for i, (col, series_id) in enumerate(zip([col1, col2, col3, col4],
                                                   ['GDP', 'UNRATE', 'CPIAUCSL', 'FEDFUNDS'])):
            obs = load_fred_observations(series_id, years=1)
            if not obs.empty:
                latest = obs.iloc[-1]['value']
                prev = obs.iloc[-2]['value'] if len(obs) > 1 else latest
                delta = latest - prev

                series_info = series_df[series_df['series_id'] == series_id].iloc[0]

                with col:
                    st.metric(
                        label=series_id,
                        value=f"{latest:,.2f}",
                        delta=f"{delta:+.2f}",
                        help=series_info['title']
                    )

        st.divider()

        # Time range selector
        years = st.slider("Years of history", 1, 50, 10)

        # Charts
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("GDP (Quarterly)")
            gdp_df = load_fred_observations('GDP', years)
            if not gdp_df.empty:
                fig = px.area(gdp_df, x='observation_date', y='value',
                             labels={'observation_date': 'Date', 'value': 'Billions USD'})
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Unemployment Rate")
            unrate_df = load_fred_observations('UNRATE', years)
            if not unrate_df.empty:
                fig = px.line(unrate_df, x='observation_date', y='value',
                             labels={'observation_date': 'Date', 'value': 'Percent'})
                fig.update_traces(line_color='#ff6b6b')
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Consumer Price Index")
            cpi_df = load_fred_observations('CPIAUCSL', years)
            if not cpi_df.empty:
                fig = px.line(cpi_df, x='observation_date', y='value',
                             labels={'observation_date': 'Date', 'value': 'Index (1982-84=100)'})
                fig.update_traces(line_color='#4ecdc4')
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Federal Funds Rate")
            ff_df = load_fred_observations('FEDFUNDS', years)
            if not ff_df.empty:
                fig = px.line(ff_df, x='observation_date', y='value',
                             labels={'observation_date': 'Date', 'value': 'Percent'})
                fig.update_traces(line_color='#a55eea')
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)

# ===========================================
# TAB 2: Hacker News
# ===========================================
with tab2:
    st.header("Hacker News Top Stories")

    stories_df = load_hn_stories()

    if stories_df.empty:
        st.warning("No HN data found. Run ingest_hn.py first.")
    else:
        stats_df = load_hn_stats()

        # Metrics
        col1, col2, col3, col4 = st.columns(4)

        story_count = len(stories_df)
        total_comments = stories_df['descendants'].sum()
        avg_score = stories_df['score'].mean()
        top_score = stories_df['score'].max()

        col1.metric("Stories", story_count)
        col2.metric("Total Comments", f"{total_comments:,}")
        col3.metric("Avg Score", f"{avg_score:.1f}")
        col4.metric("Top Score", top_score)

        st.divider()

        col1, col2 = st.columns([2, 1])

        with col1:
            st.subheader("Top Stories by Score")
            top_stories = stories_df.head(15)[['title', 'score', 'descendants', 'by']]
            top_stories.columns = ['Title', 'Score', 'Comments', 'Author']
            st.dataframe(top_stories, use_container_width=True, hide_index=True)

        with col2:
            st.subheader("Score Distribution")
            fig = px.histogram(stories_df, x='score', nbins=20,
                              labels={'score': 'Score', 'count': 'Stories'})
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("Top Authors")
            authors_df = load_hn_top_authors()
            if not authors_df.empty:
                fig = px.bar(authors_df.head(5), x='author', y='total_score',
                            labels={'author': 'Author', 'total_score': 'Total Score'})
                fig.update_layout(height=250)
                st.plotly_chart(fig, use_container_width=True)

        # Score vs Comments scatter
        st.subheader("Score vs Comments")
        fig = px.scatter(stories_df, x='score', y='descendants',
                        hover_data=['title', 'by'],
                        labels={'score': 'Score', 'descendants': 'Comments'})
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

# ===========================================
# TAB 3: Data Explorer
# ===========================================
with tab3:
    st.header("SQL Query Explorer")

    st.markdown("""
    Available tables:
    - `fred_series` - FRED series metadata
    - `fred_observations` - FRED time series data
    - `hn_items` - Hacker News stories and comments
    - `hn_users` - HN user profiles
    - `sync_metadata` - Ingestion tracking
    """)

    default_query = """SELECT
    s.series_id,
    s.title,
    COUNT(*) as observations,
    MIN(o.observation_date) as first_date,
    MAX(o.observation_date) as last_date
FROM fred_series s
JOIN fred_observations o ON s.series_id = o.series_id
GROUP BY s.series_id, s.title"""

    query = st.text_area("SQL Query", value=default_query, height=150)

    if st.button("Run Query", type="primary"):
        try:
            conn = get_connection()
            result = conn.execute(query).fetchdf()
            st.dataframe(result, use_container_width=True)
            st.caption(f"{len(result)} rows returned")
        except Exception as e:
            st.error(f"Query error: {e}")

# Footer
st.divider()
st.caption(f"Data from warehouse.duckdb | Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
