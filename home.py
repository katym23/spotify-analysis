import streamlit as st
import os, uuid
import pandas as pd
from spotify_funcs import *

def show():
    # App title
    st.header("Spotify Listening Activity")

    # Ask for user ID
    user_id = st.text_input("Enter your name or ID to begin:", key="user_id")

    st.session_state["current_user_id"] = user_id

    if not user_id:
        st.stop()

    # Storage folder setup
    UPLOAD_DIR = "user_uploads"
    os.makedirs(UPLOAD_DIR, exist_ok=True)

    # Paths for user-specific files
    def get_user_file_path(user_id):
        return os.path.join(UPLOAD_DIR, f"{user_id}_spotify.json")

    def get_final_df_path(user_id):
        return os.path.join(UPLOAD_DIR, f"{user_id}_final.parquet")

    filepath = get_user_file_path(user_id)
    final_df_path = get_final_df_path(user_id)

    # Load existing final_df if available
    if os.path.exists(final_df_path):
        final_df = pd.read_parquet(final_df_path)
        st.info("📂 Loaded your previously processed Spotify data.")
    else:
        # No processed file — check for raw JSON or prompt upload
        if not os.path.exists(filepath):
            st.write("Welcome to your own Spotify listening analysis! To get started, please upload your Spotify extended streaming history below. If you don't have your history, you can request it [here.](https://www.spotify.com/ca-en/account/privacy/)")

            uploaded_files = st.file_uploader("Upload your Spotify Extended Listening History JSON files here:", type="json", accept_multiple_files=True)

            if uploaded_files:
                df = read_spotify_json(uploaded_files)
                df.to_json(filepath, orient="records")  # save raw file
                st.success("✅ Uploaded and saved your data.")
            else:
                st.warning("No file uploaded and no saved data found. Please upload your data.")
                st.stop()
        else:
            df = pd.read_json(filepath)
            st.info("📄 Loaded your raw uploaded data. Now processing...")

        # Only process if not already done
        @st.cache_data(show_spinner="🔄 Processing your Spotify data...")
        def generate_spotify_df(df, client_id, client_secret):
            df = combine_raw_meta(df, client_id, client_secret)
            df = clean_listening_data(df)
            df = first_year_listened(df)
            df = clean_spdata_for_analysis(df)
            return df

        final_df = generate_spotify_df(df, client_id='dd688622a6b44aa78c503738e7d6cd5d',
                                        client_secret='50b1bd9dc21944f8af7b585ab97869a0')
        
        # cast uuids to strings
        # Convert UUID columns to strings (or any unsupported type)
        for col in final_df.columns:
            if final_df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
                final_df[col] = final_df[col].astype(str)


        final_df.to_parquet(final_df_path)
        st.success("🎉 Your data has been processed and saved!")

    # Optional: Delete all user files
    if st.button("🗑️ Delete my stored file"):
        if os.path.exists(filepath):
            os.remove(filepath)
        if os.path.exists(final_df_path):
            os.remove(final_df_path)
        st.success("Your data has been deleted. Please reload the page to upload again.")
        st.stop()

    # Show the final dataset
    st.write("🎧 Final Dataset:")
    st.dataframe(final_df)
    st.write("Go check out the other tabs for an analysis of your data :-)")