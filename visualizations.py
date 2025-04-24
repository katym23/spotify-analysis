import streamlit as st
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go

def show():
    st.title("📊 Visualize Your Spotify Data")

    # ✅ Get user ID from session state
    if "current_user_id" not in st.session_state:
        st.warning("Please enter your ID on the Home page first.")
        st.stop()

    user_id = st.session_state["current_user_id"]

    UPLOAD_DIR = "user_uploads"
    final_df_path = os.path.join(UPLOAD_DIR, f"{user_id}_final.parquet")

    # Load the processed final_df instead of reprocessing
    if not os.path.exists(final_df_path):
        st.warning("⚠️ No processed data found for this user. Please go to the Home page and upload your data first.")
        st.stop()

    final_df = pd.read_parquet(final_df_path)

    # === Visualizations ===

    # # Date range picker

    # # Make sure timestamp_listened is datetime
    # final_df['timestamp_listened'] = pd.to_datetime(final_df['timestamp_listened'], errors='coerce')
    # final_df['date_listened'] = final_df['timestamp_listened'].dt.date

    # # Get min and max as datetime.date
    # min_date = final_df['date_listened'].min().date()
    # max_date = final_df['date_listened'].max().date()

    # st.write(final_df['date_listened'].dtype)

    # # Let user pick date range
    # date_range = st.date_input(
    #     "Filter by date range:",
    #     [min_date, max_date],
    #     min_value=min_date,
    #     max_value=max_date
    # )

    # # Convert date_range (datetime.date) to datetime64[ns]
    # start = pd.to_datetime(date_range[0])
    # end = pd.to_datetime(date_range[1])

    # # Apply filter
    # filtered_df = final_df[
    #     (final_df['timestamp_listened'] >= start) &
    #     (final_df['timestamp_listened'] <= end)
    # ]

    st.subheader("🎤 Top Artists")
    top_artists = final_df['artist'].value_counts().head(10).sort_values(ascending=False)
    top_artists_chart = px.bar(top_artists)
    st.plotly_chart(top_artists_chart)

    st.subheader("📅 Listening Activity Over Time")

    final_df['year'] = final_df['timestamp_listened'].dt.year
    final_df['month'] = final_df['timestamp_listened'].dt.month
    monthly = final_df.groupby(['year', 'month']).size().reset_index(name='Plays')
    monthly['Date'] = pd.to_datetime(monthly[['year', 'month']].assign(DAY=1))
    listening_activity_chart = px.line(monthly, x='Date', y='Plays')
    st.plotly_chart(listening_activity_chart)

    st.subheader("Top Genres")
    top_genres = final_df['general_genre'].value_counts().head(10).sort_values(ascending=False)
    top_genres_chart = px.bar(top_genres)
    st.plotly_chart(top_genres_chart)