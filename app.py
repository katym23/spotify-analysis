import streamlit as st
import home
import visualizations  # import other modules as needed
import table

# App title
st.set_page_config(page_title="Spotify Dashboard", layout="wide")

# Create tabs
tabs = st.tabs(["🏠 Home", "📊 Analysis", "Full Data"])

# Assign modules to tabs
with tabs[0]:
    home.show()

with tabs[1]:
    visualizations.show()

with tabs[2]:
    table.show()