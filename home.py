import streamlit as st
import os, uuid
import pandas as pd
from spotify_funcs import *
from supabase import create_client, Client

def show():
    # App title
    st.header("Spotify Listening Activity")

    # Ask for user ID
    user_id = st.text_input("Enter your name or ID to begin:", key="user_id")
    st.session_state["current_user_id"] = user_id
    parquet_path = f"{user_id}/final_df.parquet"  # The file path where the user data is stored

    if not user_id:
        st.stop()

    # 1. Check if Parquet file already exists in the user-specific path

    parquet_exists = file_exists_in_bucket("user-data", parquet_path)
    if parquet_exists:
        st.success("✅ Found existing data! Loading it...")
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

        st.success("✅ All files read. Processing your Spotify data...")

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

        st.success("🎉 Your Spotify data has been processed and saved!")

    # Show the final dataset
    st.write("🎧 Final Dataset:")
    st.dataframe(df)
    st.write("Go check out the other tabs for an analysis of your data :-)")
