import streamlit as st
import os
from dotenv import load_dotenv

# --- Page Configuration ---
st.set_page_config(
    page_title="E-commerce Dashboard",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- Load environment variables ---
load_dotenv()

# Custom CSS for buttons and general styling
st.markdown("""
<style>
    /* Navigation Buttons */
    .nav-button {
        display: inline-block;
        padding: 0.75rem 1.5rem;
        margin: 0.5rem 0;
        border-radius: 0.5rem;
        background-color: #4f46e5;
        color: white !important; /* Use !important to ensure override */
        font-weight: 600;
        text-decoration: none;
        text-align: center;
        transition: all 0.2s;
        border: none;
        cursor: pointer;
        width: 100%;
    }
    .nav-button:hover {
        background-color: #4338ca;
        transform: translateY(-2px);
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
    }
    .nav-button.secondary {
        background-color: #f3f4f6;
        color: #1f2937 !important; /* Use !important to ensure override */
    }
    .nav-button.secondary:hover {
        background-color: #e5e7eb;
    }

    /* Main Header and Welcome Text */
    .main-header {
        font-size: 2.5rem;
        text-align: center;
        margin-bottom: 1.5rem;
        color: var(--text-color); /* Use Streamlit's text color variable */
    }
    .welcome-text {
        font-size: 1.2rem;
        line-height: 1.6;
        margin-bottom: 2rem;
        color: var(--text-color); /* Use Streamlit's text color variable */
    }

    /* Feature Card Styling */
    .feature-card {
        padding: 1.5rem;
        border-radius: 10px;
        /* Use Streamlit's background color variables for adaptability */
        background-color: var(--secondary-background-color); 
        margin-bottom: 1rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border: 1px solid var(--border-color); /* Add a subtle border for definition */
    }
    .feature-title {
        color: var(--primary-color); /* Use Streamlit's primary color for titles */
        font-size: 1.3rem;
        margin-bottom: 0.5rem;
    }
    .feature-card p {
        color: var(--text-color); /* Ensure paragraph text also adapts */
    }
</style>
""", unsafe_allow_html=True)

# Main content
st.markdown("<h1 class='main-header'>Welcome to E-commerce Analytics Dashboard</h1>", unsafe_allow_html=True)

st.markdown("""
<div class='welcome-text'>
    This dashboard provides comprehensive analytics for an e-commerce platform.
    Select an option below to explore real-time metrics or historical performance data.
</div>
""", unsafe_allow_html=True)

# Navigation buttons
col1, col2 = st.columns(2)

with col1:
    if st.button("📈 Go to Realtime Analytics", key="realtime_btn", use_container_width=True):
        st.switch_page("pages/📈Realtime_Analytics.py")

    st.markdown("""
    <div class='feature-card'>
        <div class='feature-title'>📈 Realtime Analytics</div>
        <p>Monitor live metrics, user activity, and sales as they happen.
        Track product views, cart additions, and orders in real-time.</p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    if st.button("📊 Go to Historical Analytics", key="historical_btn", use_container_width=True):
        st.switch_page("pages/📊Historical_Analytics.py")

    st.markdown("""
    <div class='feature-card'>
        <div class='feature-title'>📊 Historical Analytics</div>
        <p>Analyze historical trends, customer behavior, and sales performance
        over time. Gain insights from data warehouse.</p>
    </div>
    """, unsafe_allow_html=True)

# Footer
st.markdown("---")
st.caption("© 2025 E-commerce Analytics Dashboard | Built for capstone project by Rojan | TechKraft Inc.")