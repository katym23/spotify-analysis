import streamlit as st
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go
from spotify_funcs import *

# Function definitions
def plot_bar_chart():
    return

def plot_line_chart(df, x, y, color=None):
    line_chart = px.line(df, x=x, y=y, color=color)
    st.plotly_chart(line_chart)

def show():
    st.title("📊 Visualize Your Spotify Data")
    sub_tabs = st.tabs(["Top Artists", "Timeline", "Genres"])

    if 'filtered_df' not in st.session_state:
        st.warning("⚠️ No data found. Please upload data on the home page!")
        st.stop()
    else:
        filtered_df = st.session_state.filtered_df

    # === Visualizations ===

    with sub_tabs[0]:
        st.subheader("🎤 Top Picks")

        y_axis_attr = st.sidebar.selectbox("Choose a feature: ", ["artist", "track-artist", "album", "general_genre"])
        tooltip_attrs = st.sidebar.multiselect("Select attributes for tooltip:", ["artist", "tracks", "albums", "genres"])
        tooltip_attrs = {attr: True for attr in tooltip_attrs if attr != y_axis_attr}  # Exclude the y-axis attribute
        agg_df_multi = (
            filtered_df
            .groupby(y_axis_attr, as_index=False)
            .agg(
                count  = ("artist", "size"),
                genres = ("general_genre", lambda x: ", ".join(x.unique())),
                tracks = ("track", lambda x: ", ".join(x)),
                albums = ("album", lambda x: ", ".join(x.unique())),
                artist = ("artist", lambda x: ", ".join(x.unique())),
            )
        ).sort_values(by='count', ascending=False)
        plot_data = agg_df_multi.head(10).sort_values(by='count')  # Limit to top 10 for better visualization
        fig = px.bar(
            data_frame=plot_data,
            y=y_axis_attr,
            x='count',
            hover_data = tooltip_attrs
        )
        fig.update_xaxes(categoryorder="category descending")
        st.plotly_chart(fig)

    with sub_tabs[1]:
        st.subheader("📅 Listening Activity Over Time")
        
        # Checkbox to show genre plays
        show_genre_marks = st.checkbox("Show Genre Plays", value=False)
        
        # Only show genre selection if the checkbox is checked
        if show_genre_marks:
            selected_genre = st.selectbox("Choose a genre to highlight:", filtered_df['general_genre'].unique())
            
            filtered_df['date_listened'] = filtered_df['timestamp_listened'].dt.date
            total_plays = (
                filtered_df
                .groupby('date_listened')
                .size()
                .reset_index(name='TotalPlays')
            )
            genre_plays = (
                filtered_df[filtered_df['general_genre'] == selected_genre]
                .groupby('date_listened')
                .size()
                .reset_index(name='GenrePlays')
            )
            daily = pd.merge(total_plays, genre_plays, on='date_listened', how='left')
            daily['GenrePlays'] = daily['GenrePlays'].fillna(0)

            fig = go.Figure()

            # Line for total plays
            fig.add_trace(go.Scatter(
                x=daily['date_listened'],
                y=daily['TotalPlays'],
                mode='lines',
                name='Total Plays',
                line=dict(color='lightgrey')
            ))

            # If the user wants to see genre plays, add it to the graph
            fig.add_trace(go.Scatter(
                x=daily['date_listened'],
                y=daily['GenrePlays'],
                mode='markers',
                name=f'{selected_genre} Plays',
                marker=dict(size=8, color='darkblue')
            ))

            # Layout
            fig.update_layout(
                title=f"Listening Activity",
                yaxis=dict(title='Number of Plays'),
                legend=dict(x=0.01, y=0.99)
            )

            st.plotly_chart(fig)
        else:

            filtered_df['date_listened'] = filtered_df['timestamp_listened'].dt.date
            total_plays = (
                filtered_df
                .groupby('date_listened')
                .size()
                .reset_index(name='TotalPlays')
            )
            
            fig = go.Figure()

            # Line for total plays
            fig.add_trace(go.Scatter(
                x=total_plays['date_listened'],
                y=total_plays['TotalPlays'],
                mode='lines',
                name='Total Plays',
                line=dict(color='lightgrey')
            ))

            # Layout
            fig.update_layout(
                title="Listening Activity",
                yaxis=dict(title='Number of Plays'),
                legend=dict(x=0.01, y=0.99)
            )

            st.plotly_chart(fig)

    with sub_tabs[2]:
        st.subheader("Top Genres")
        top_genres = filtered_df['general_genre'].value_counts().head(10).sort_values(ascending=False)
        top_genres_chart = px.bar(top_genres)
        st.plotly_chart(top_genres_chart)