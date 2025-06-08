import streamlit as st
import pandas as pd
from deltalake import DeltaTable
import os
from dotenv import load_dotenv
from datetime import timedelta

# --- Load Environment Variables ---
load_dotenv()

# --- Page Configuration ---
st.set_page_config(
    page_title="YouTube Poland Trending Analysis",
    page_icon="📊",
    layout="wide"
)

# --- Azure Storage Connection ---
storage_account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
storage_account_key = os.getenv("AZURE_STORAGE_KEY")

storage_options = {"account_name": storage_account_name, "account_key": storage_account_key}

# --- Data Loading Function with Caching ---
@st.cache_data
def load_data(table_name):
    """Function to load a specific delta table from the Gold layer."""
    try:
        path = f"az://gold/{table_name}"
        dt = DeltaTable(path, storage_options=storage_options)
        df = dt.to_pandas()
        return df
    except Exception as e:
        st.error(f"Failed to load data from {table_name}. Error: {e}")
        return pd.DataFrame()

# --- Load All DataFrames ---
df_daily_category = load_data("daily_category_performance")
df_channel_perf = load_data("channel_performance")
df_top_videos = load_data("daily_most_liked_video")

# --- Data Processing ---
if not df_daily_category.empty:
    df_daily_category['ingestion_date'] = pd.to_datetime(df_daily_category['ingestion_date'], errors='coerce')
if not df_top_videos.empty:
    df_top_videos['ingestion_date'] = pd.to_datetime(df_top_videos['ingestion_date'], errors='coerce')

# --- Main Application ---
st.title("📊 YouTube Poland Trending Analysis")
st.markdown("This interactive dashboard analyzes daily trending video data from YouTube in Poland.")

# --- Sidebar for Filters ---
st.sidebar.header("Filters")

if not df_daily_category.empty:
    min_date = df_daily_category['ingestion_date'].min().date()
    max_date = df_daily_category['ingestion_date'].max().date()

    selected_date_range = st.sidebar.slider("Select Date Range", min_value=min_date, max_value=max_date, value=(max_date - timedelta(days=7), max_date), format="YYYY-MM-DD")
    
    unique_categories = sorted(df_daily_category['category_name'].dropna().unique())
    
    selected_categories = st.sidebar.multiselect("Select Video Categories", options=unique_categories, default=unique_categories)
else:
    st.sidebar.warning("Daily data not available for filtering.")
    selected_date_range = (None, None)
    selected_categories = []

# --- Data Filtering Logic ---
snapshot_date = selected_date_range[1] if selected_date_range[1] else max_date

# Filter for KPIs and daily charts
if snapshot_date and not df_daily_category.empty:
    df_snapshot = df_daily_category[df_daily_category['ingestion_date'].dt.date == snapshot_date]
    if selected_categories:
        df_snapshot = df_snapshot[df_snapshot['category_name'].isin(selected_categories)]
else:
    df_snapshot = pd.DataFrame()

# Filter for the time series chart
if selected_date_range[0] and selected_date_range[1] and not df_daily_category.empty:
    df_time_series = df_daily_category[(df_daily_category['ingestion_date'].dt.date >= selected_date_range[0]) & (df_daily_category['ingestion_date'].dt.date <= selected_date_range[1])]
    if selected_categories:
        df_time_series = df_time_series[df_time_series['category_name'].isin(selected_categories)]
else:
    df_time_series = pd.DataFrame()

# --- Page Layout ---
st.header(f"Daily Snapshot for {snapshot_date.strftime('%B %d, %Y')}")

if not df_snapshot.empty:
    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric(label="Total Views", value=f"{df_snapshot['total_views'].sum():,}")
    kpi2.metric(label="Total Likes", value=f"{df_snapshot['total_likes'].sum():,}")
    kpi3.metric(label="Trending Videos", value=int(df_snapshot['video_count'].sum()))
else:
    st.info("No data available for the selected date and categories to display KPIs.")

# --- New: Category & Video Details Columns ---
col1, col2 = st.columns((2, 1.5)) # Create two columns, the first one wider

with col1:
    st.subheader("Views by Category")
    if not df_snapshot.empty:
        st.bar_chart(df_snapshot.set_index('category_name')['total_views'])
    else:
        st.info("No data to display.")

with col2:
    st.subheader("Most Liked Video")
    if not df_top_videos.empty and snapshot_date:
        top_video = df_top_videos[df_top_videos['ingestion_date'].dt.date == snapshot_date]
        if not top_video.empty:
            video_info = top_video.iloc[0]
            st.markdown(f"**{video_info['video_title']}**")
            st.markdown(f"by *{video_info['channel_title']}*")
            st.markdown(f"👍 {video_info['like_count']:,} Likes")
            st.video(f"https://www.youtube.com/watch?v={video_info['video_id']}")
        else:
            st.info("No top video found for this day.")
    else:
        st.info("Top video data not available.")

# --- Time Series and Leaderboard ---
st.header("Performance Over Time")
if not df_time_series.empty:
    df_pivot = df_time_series.pivot_table(index='ingestion_date', columns='category_name', values='total_views', aggfunc='sum').fillna(0)
    st.line_chart(df_pivot)
else:
    st.info("No data available for the selected date range and categories to display trends.")

st.header("All-Time Channel Leaderboard")
if not df_channel_perf.empty:
    st.dataframe(df_channel_perf.nlargest(10, 'total_views'))
else:
    st.warning("Could not load all-time channel performance data.")