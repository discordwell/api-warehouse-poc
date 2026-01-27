#!/usr/bin/env python3
"""
Hacker News Dashboard

Visualizes top stories and discussion trends.
Run with: streamlit run dashboard_hn.py
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime

st.set_page_config(
    page_title="Hacker News Dashboard",
    page_icon="📰",
    layout="wide"
)

DB_PATH = Path(__file__).parent / "warehouse.duckdb"


@st.cache_resource
def get_connection():
    return duckdb.connect(str(DB_PATH), read_only=True)


def load_stories():
    conn = get_connection()
    return conn.execute("""
        SELECT id, title, score, descendants, "by", time, url,
               to_timestamp(time) as posted_at
        FROM hn_items
        WHERE type = 'story'
        ORDER BY score DESC
    """).fetchdf()


def load_comments():
    conn = get_connection()
    return conn.execute("""
        SELECT id, "by", time, parent, text
        FROM hn_items
        WHERE type = 'comment'
    """).fetchdf()


def load_item_stats():
    conn = get_connection()
    return conn.execute("""
        SELECT type, COUNT(*) as count
        FROM hn_items
        GROUP BY type
    """).fetchdf()


def load_top_authors():
    conn = get_connection()
    return conn.execute("""
        SELECT
            "by" as author,
            COUNT(*) as posts,
            SUM(CASE WHEN type = 'story' THEN score ELSE 0 END) as total_score,
            SUM(CASE WHEN type = 'story' THEN 1 ELSE 0 END) as stories,
            SUM(CASE WHEN type = 'comment' THEN 1 ELSE 0 END) as comments
        FROM hn_items
        WHERE "by" IS NOT NULL
        GROUP BY "by"
        ORDER BY total_score DESC
        LIMIT 20
    """).fetchdf()


def extract_domain(url):
    if pd.isna(url) or not url:
        return "self"
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc.replace('www.', '')
    except:
        return "unknown"


# Header
st.title("📰 Hacker News Dashboard")
st.caption("Top stories and discussion analysis")

stories_df = load_stories()

if stories_df.empty:
    st.error("No HN data found. Run `python ingest_hn.py` first.")
    st.stop()

# Stats row
stats = load_item_stats()
story_count = stats[stats['type'] == 'story']['count'].values[0] if len(stats[stats['type'] == 'story']) > 0 else 0
comment_count = stats[stats['type'] == 'comment']['count'].values[0] if len(stats[stats['type'] == 'comment']) > 0 else 0

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("📖 Stories", story_count)
col2.metric("💬 Comments", f"{comment_count:,}")
col3.metric("⬆️ Avg Score", f"{stories_df['score'].mean():.0f}")
col4.metric("🔥 Top Score", stories_df['score'].max())
col5.metric("💬 Most Comments", stories_df['descendants'].max())

st.divider()

# Main content
tab1, tab2, tab3 = st.tabs(["🔥 Top Stories", "👥 Authors", "📊 Analytics"])

with tab1:
    st.header("Top Stories")

    # Sorting options
    col1, col2 = st.columns([1, 4])
    with col1:
        sort_by = st.selectbox("Sort by", ["Score", "Comments", "Recent"])

    if sort_by == "Score":
        display_df = stories_df.sort_values('score', ascending=False)
    elif sort_by == "Comments":
        display_df = stories_df.sort_values('descendants', ascending=False)
    else:
        display_df = stories_df.sort_values('time', ascending=False)

    # Display stories
    for i, row in display_df.head(20).iterrows():
        with st.container():
            col1, col2 = st.columns([4, 1])
            with col1:
                domain = extract_domain(row['url']) if row['url'] else "self"
                st.markdown(f"**{row['title']}**")
                st.caption(f"🔗 {domain} | 👤 {row['by']} | 🕐 {row['posted_at'].strftime('%Y-%m-%d %H:%M') if pd.notna(row['posted_at']) else 'N/A'}")
            with col2:
                st.metric("Score", row['score'], label_visibility="collapsed")
                st.caption(f"💬 {row['descendants'] or 0}")
            st.divider()

with tab2:
    st.header("Top Contributors")

    authors_df = load_top_authors()

    if not authors_df.empty:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("By Total Score")
            fig = px.bar(authors_df.head(10), x='total_score', y='author',
                        orientation='h', title="Top 10 by Score")
            fig.update_layout(height=400, yaxis={'categoryorder': 'total ascending'})
            fig.update_traces(marker_color='#ff6600')
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("By Activity")
            fig = px.bar(authors_df.head(10), x='posts', y='author',
                        orientation='h', title="Top 10 by Posts")
            fig.update_layout(height=400, yaxis={'categoryorder': 'total ascending'})
            fig.update_traces(marker_color='#3498db')
            st.plotly_chart(fig, use_container_width=True)

        # Author table
        st.subheader("Author Breakdown")
        st.dataframe(
            authors_df[['author', 'stories', 'comments', 'total_score']],
            use_container_width=True,
            hide_index=True,
            column_config={
                'author': 'Author',
                'stories': 'Stories',
                'comments': 'Comments',
                'total_score': 'Total Score'
            }
        )

with tab3:
    st.header("Analytics")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Score Distribution")
        fig = px.histogram(stories_df, x='score', nbins=30,
                          title="Story Score Distribution")
        fig.update_traces(marker_color='#ff6600')
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Comments Distribution")
        fig = px.histogram(stories_df, x='descendants', nbins=30,
                          title="Comment Count Distribution")
        fig.update_traces(marker_color='#3498db')
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    # Score vs Comments scatter
    st.subheader("Score vs Comments Correlation")
    fig = px.scatter(stories_df, x='score', y='descendants',
                    hover_data=['title', 'by'],
                    title="Do high-scoring posts get more comments?",
                    labels={'score': 'Score', 'descendants': 'Comments'})
    fig.update_traces(marker=dict(color='#ff6600', size=10, opacity=0.6))
    fig.update_layout(height=450)

    # Add trendline
    if len(stories_df) > 2:
        import numpy as np
        z = np.polyfit(stories_df['score'].fillna(0), stories_df['descendants'].fillna(0), 1)
        p = np.poly1d(z)
        x_line = [stories_df['score'].min(), stories_df['score'].max()]
        fig.add_trace(go.Scatter(x=x_line, y=p(x_line), mode='lines',
                                name='Trend', line=dict(color='gray', dash='dash')))

    st.plotly_chart(fig, use_container_width=True)

    # Domain analysis
    st.subheader("Top Domains")
    stories_df['domain'] = stories_df['url'].apply(extract_domain)
    domain_counts = stories_df['domain'].value_counts().head(10)

    fig = px.pie(values=domain_counts.values, names=domain_counts.index,
                title="Stories by Domain")
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

# Footer
st.divider()
st.caption(f"Data source: Hacker News API | Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
