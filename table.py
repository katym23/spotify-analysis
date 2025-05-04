import pandas as pd
import streamlit as st

def show():
    if 'filtered_df' not in st.session_state:
        st.warning("⚠️ No data found. Please upload data on the home page!")
        st.stop()
    else:
        final_df = st.session_state.filtered_df

    # Add the date picker for selecting a specific date, defaulting to None
    selected_date = st.date_input("Pick a date", value=None)  # Default is None, showing full data initially

    # If a date is selected, filter by it; otherwise, show all data
    if selected_date:
        final_df['timestamp_listened'] = pd.to_datetime(final_df['timestamp_listened'])
        final_df = final_df[final_df['timestamp_listened'].dt.date == selected_date]

    # Add the artist selectbox, allowing the user to filter by artist
    artist_selection = st.selectbox('Artist:', options=['All'] + list(final_df['artist'].unique()), index=0)

    # If an artist is selected, filter by artist
    if artist_selection != 'All':
        final_df = final_df[final_df['artist'] == artist_selection]
    
    # Sort by timestamp_listened in descending order
    final_df = final_df.sort_values(by='timestamp_listened', ascending=False)

    # Display the filtered dataframe
    st.dataframe(final_df, height=750)