import streamlit as st
import home
import visualizations  # import other modules as needed

# App title
st.set_page_config(page_title="Spotify Dashboard", layout="wide")

# Create tabs
tabs = st.tabs(["🏠 Home", "📊 Analysis"])

# Assign modules to tabs
with tabs[0]:
    home.show()

with tabs[1]:
    visualizations.show()
