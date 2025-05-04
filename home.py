import streamlit as st
import uuid
import pandas as pd
from spotify_funcs import *
import plotly.express as px
import plotly.graph_objects as go

def show():
    # App title
    st.header("Spotify Listening Activity")

    # Ask for user ID
    user_id = st.text_input("Enter your name or ID to begin:", key="user_id")
    parquet_path = f"{user_id}/final_df.parquet"  # The file path where the user data is stored

    if not user_id:
        st.stop()

    st.session_state["current_user_id"] = user_id

    # Create a placeholder for the success message
    success_message = st.empty()

    # 1. Check if Parquet file already exists in the user-specific path
    parquet_exists = file_exists_in_bucket("user-data", parquet_path)
    if parquet_exists:
        # Display the success message
        success_message.success("✅ Found existing data! Loading it...")
        df = load_df_from_supabase(user_id)
        st.session_state.spotify_df = df
    else:
        st.warning("⚠️ No saved Spotify data found for your ID.")

        # Let the user upload JSON files if no data found
        uploaded_files = st.file_uploader(
            "Upload your Spotify Extended Listening History JSON files:",
            type="json",
            accept_multiple_files=True
        )

        if not uploaded_files:
            st.stop()

        # 4. Progress bar for processing
        total = len(uploaded_files)
        progress_bar = st.progress(0)

        all_json_records = []
        for idx, f in enumerate(uploaded_files):
            # Read and process each file
            all_json_records.extend(read_spotify_json([f]).to_dict(orient="records"))
            progress_bar.progress((idx + 1) / total)

        # Success after all files read
        success_message.success("✅ All files read. Processing your Spotify data...")

        # 5. Build full dataframe from the JSON records
        df = pd.DataFrame(all_json_records)

        # 6. Cache the processing function
        @st.cache_data(show_spinner="🔄 Processing your Spotify data...")
        def generate_spotify_df(df, client_id, client_secret):
            df = combine_raw_meta(df, client_id, client_secret)
            df = clean_listening_data(df)
            df = first_year_listened(df)
            df = clean_spdata_for_analysis(df)
            return df

        df = generate_spotify_df(df, client_id=default_id, client_secret=default_secret)

        if 'spotify_df' not in st.session_state:
            st.session_state.spotify_df = df

        # Fix UUID columns if needed
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
                df[col] = df[col].astype(str)

        # 7. Save the final dataframe as Parquet
        save_df_to_supabase(user_id, df)

        success_message.success("🎉 Your Spotify data has been processed and saved!")

    # Create columns for layout
    left_col, right_col = st.columns([1, 2])

    # Extract date components
    df['year'] = df['timestamp_listened'].dt.year
    df['quarter'] = df['timestamp_listened'].dt.to_period('Q').astype(str)
    df['month'] = df['timestamp_listened'].dt.month
    df['month_name'] = df['timestamp_listened'].dt.strftime('%B')

    # Required: Year dropdown
    selected_year = st.sidebar.selectbox("Select year:", ["All"] + (sorted(df['year'].unique(), reverse=True)))

    # Filter to selected year
    if selected_year == 'All':
        filtered_df = df.copy()
    else:
        filtered_df = df[df['year'] == selected_year]
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

    st.session_state["filtered_df"] = filtered_df

    # build dashboard
    with left_col:
        sunburst_df = filtered_df.groupby(['general_genre', 'genre1', 'artist'])['track_id'].count().reset_index()
        sunburst_df = sunburst_df.sort_values(by='track_id', ascending=False).head(25)
        sunburst_df.rename(columns={'general_genre': 'Genre',
                                    'genre1': 'Sub Genre',
                                    'artist': 'Artist',
                                    'track_id': 'Listens'}, inplace=True)
        fig = px.sunburst (
            sunburst_df,
            path=['Genre', 'Artist'],
            values='Listens',
            color='Genre',
            hover_data='Sub Genre',
            color_discrete_sequence=px.colors.qualitative.Set3
        )

        fig.update_layout(margin=dict(t=10, l=10, r=10, b=10))
        st.plotly_chart(fig)

    with right_col:
        filtered_df['date_listened'] = filtered_df['timestamp_listened'].dt.date
        daily_listens_df = filtered_df['date_listened'].value_counts().sort_index()
        daily_listens_df = daily_listens_df.reset_index()
        daily_listens_df.columns = ['Date', 'Listens']

        # Plot with Plotly
        fig = px.bar(daily_listens_df, x='Date', y='Listens', title='Listening Activity by Day', color_discrete_sequence=[px.colors.qualitative.Set3[2]])
        fig.update_layout(xaxis_title='Date', yaxis_title='Number of Songs Played')

        # Show in Streamlit
        st.plotly_chart(fig, use_container_width=True)
