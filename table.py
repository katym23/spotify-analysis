import pandas as pd
import streamlit as st

def show():
    if 'spotify_df' not in st.session_state:
        st.warning("⚠️ No data found. Please upload data on the home page!")
        st.stop()
    else:
        final_df = st.session_state.spotify_df

    final_df = final_df.sort_values(by='timestamp_listened', ascending=False)

    st.dataframe(final_df, height=750)