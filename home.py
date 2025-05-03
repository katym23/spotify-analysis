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
    # st.write("🎧 Final Dataset:")
    # st.dataframe(df)
    # st.write("Go check out the other tabs for an analysis of your data :-)")

    # build dashboard
    # y_axis_attr = st.sidebar.selectbox("Choose a feature: ", ["artist", "track-artist", "album", "general_genre"])
    # tooltip_attrs = st.sidebar.multiselect("Select attributes for tooltip:", ["artist", "tracks", "albums", "genres"])
    # tooltip_attrs = {attr: True for attr in tooltip_attrs if attr != y_axis_attr}  # Exclude the y-axis attribute
    # # plot_data = filtered_df.groupby(y_axis_attr).size().reset_index(name='count').sort_values(by='count', ascending=False)
    # agg_df_multi = (
    #     df
    #     .groupby(y_axis_attr, as_index=False)
    #     .agg(
    #         count  = ("artist", "size"),
    #         genres = ("general_genre", lambda x: ", ".join(x.unique())),
    #         tracks = ("track", lambda x: ", ".join(x)),
    #         albums = ("album", lambda x: ", ".join(x.unique())),
    #         artist = ("artist", lambda x: ", ".join(x.unique())),
    #     )
    # ).sort_values(by='count', ascending=False)
    # plot_data = agg_df_multi.head(10).sort_values(by='count')  # Limit to top 10 for better visualization
    # fig = px.bar(
    #     data_frame=plot_data,
    #     y=y_axis_attr,
    #     x='count',
    #     hover_data = tooltip_attrs
    # )
    # fig.update_xaxes(categoryorder="category descending")
    # st.plotly_chart(fig)

    sunburst_df = df.groupby(['general_genre', 'artist'])['track_id'].count().reset_index()
    sunburst_df = sunburst_df.sort_values(by='track_id', ascending=False).head(25)
    sunburst_df.rename(columns={'general_genre': 'Genre',
                                'artist': 'Artist',
                                'track_id': 'Listens'}, inplace=True)
    fig = px.sunburst (
        sunburst_df,
        path=['Genre', 'Artist'],
        values='Listens',
        color='Genre',
        color_discrete_sequence=px.colors.qualitative.Set3
    )

    fig.update_layout(margin=dict(t=10, l=10, r=10, b=10))
    st.plotly_chart(fig)