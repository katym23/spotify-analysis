import streamlit as st
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go
from spotify_funcs import *

def show():
    st.title("📊 Visualize Your Spotify Data")

    if 'spotify_df' not in st.session_state:
        st.warning("⚠️ No data found. Please upload data on the home page!")
        st.stop()
    else:
        final_df = st.session_state.spotify_df

    # === Visualizations ===

    # # Date range picker

    # Extract date components
    # Extract components
    final_df['year'] = final_df['timestamp_listened'].dt.year
    final_df['quarter'] = final_df['timestamp_listened'].dt.to_period('Q').astype(str)
    final_df['month'] = final_df['timestamp_listened'].dt.month
    final_df['month_name'] = final_df['timestamp_listened'].dt.strftime('%B')

    # Required: Year dropdown
    selected_year = st.sidebar.selectbox("Select year:", ["All"] + (sorted(final_df['year'].unique(), reverse=True)))

    # Filter to selected year
    if selected_year == 'All':
        filtered_df = final_df.copy()
    else:
        filtered_df = final_df[final_df['year'] == selected_year]
        # Optional: Quarter dropdown
        quarter_options = ["All"] + sorted(filtered_df['quarter'].unique())
        selected_quarter = st.sidebar.selectbox("Select quarter (optional):", quarter_options)

        # Determine valid months based on quarter
        quarter_month_map = {
            "Q1": [1, 2, 3],
            "Q2": [4, 5, 6],
            "Q3": [7, 8, 9],
            "Q4": [10, 11, 12]
        }

        # Default: All months in the year
        valid_months = filtered_df['month'].unique()
        if selected_quarter != "All":
            q = selected_quarter.split("Q")[1][0]  # Get '1' from 'Q1 2025'
            valid_months = quarter_month_map[f"Q{q}"]

        # Filter month options to valid months in the quarter
        valid_month_names = filtered_df[filtered_df['month'].isin(valid_months)]['month_name'].unique()
        valid_month_names = sorted(valid_month_names, key=lambda m: pd.to_datetime(m, format='%B').month)
        month_options = ["All"] + valid_month_names

        # Optional: Month dropdown (dependent on quarter)
        selected_month = st.sidebar.selectbox("Select month (optional):", month_options)

        if selected_quarter != "All":
            filtered_df = filtered_df[filtered_df['quarter'] == selected_quarter]

        if selected_month != "All":
            # Validate month is in selected quarter
            month_num = pd.to_datetime(selected_month, format='%B').month
            if selected_quarter != "All" and month_num not in valid_months:
                st.warning("Selected month is not in the selected quarter. Resetting to 'All'.")
            else:
                filtered_df = filtered_df[filtered_df['month_name'] == selected_month]

    st.subheader("🎤 Top Artists")
    top_artists = filtered_df['artist'].value_counts().head(10).sort_values(ascending=False)
    top_artists_chart = px.bar(top_artists)
    st.plotly_chart(top_artists_chart)

    st.subheader("📅 Listening Activity Over Time")

    filtered_df['date_listened'] = filtered_df['timestamp_listened'].dt.date
    daily = filtered_df.groupby('date_listened').size().reset_index(name='Plays')
    listening_activity_chart = px.line(daily, x='date_listened', y='Plays')
    st.plotly_chart(listening_activity_chart)

    st.subheader("Top Genres")
    top_genres = filtered_df['general_genre'].value_counts().head(10).sort_values(ascending=False)
    top_genres_chart = px.bar(top_genres)
    st.plotly_chart(top_genres_chart)