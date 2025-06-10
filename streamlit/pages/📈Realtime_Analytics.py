import streamlit as st
import pandas as pd
import time
import json
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from utils import (
    redis_client, fetch_data, create_timeseries_df,
    create_leaderboard_df, create_empty_chart
)

# --- Auto-refresh Controls ---
st.sidebar.title("Realtime Settings")
auto_refresh = st.sidebar.checkbox("Enable Auto-refresh", value=True, help="Automatically refresh the dashboard")
refresh_rate = st.sidebar.slider("Refresh rate (seconds)", min_value=1, max_value=60, value=5, step=1)

# Initialize session state for refresh control
if 'last_refresh_time' not in st.session_state:
    st.session_state.last_refresh_time = time.time()

# Auto-refresh logic
current_time = time.time()
time_since_last_refresh = current_time - st.session_state.last_refresh_time

if auto_refresh and time_since_last_refresh >= refresh_rate:
    st.session_state.last_refresh_time = current_time
    time.sleep(0.5)  # Brief pause
    st.rerun()

# --- Page Title ---
st.title("📊 Real-Time E-commerce Analytics")
st.markdown("Monitor live metrics, user activity, and sales as they happen.")

# --- Key Metrics Display ---
st.subheader("📈 Live Metrics Overview")
totals = fetch_data("metrics:totals")
last_updated = totals.get('last_updated', 'Never') if totals else 'Never'

# Format last updated time for display
try:
    if last_updated != 'Never':
        last_updated_dt = datetime.fromisoformat(last_updated)
        last_updated_str = last_updated_dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        last_updated_str = 'N/A'
except (ValueError, TypeError):
    last_updated_str = 'N/A'

# Show last update time and current time
current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
col_time1, col_time2 = st.columns(2)
with col_time1:
    st.caption(f"📅 **Current time:** {current_time_str}")
with col_time2:
    st.caption(f"📊 **Last data update:** {last_updated_str}")

# Display metric cards
m_col1, m_col2, m_col3, m_col4, m_col5 = st.columns(5)
with m_col1:
    views_count = int(totals.get('total_views', 0)) if totals else 0
    st.metric("👀 Total Views", f"{views_count:,}")
with m_col2:
    cart_adds = int(totals.get('total_cart_adds', 0)) if totals else 0
    st.metric("🛒 Total Cart Adds", f"{cart_adds:,}")
with m_col3:
    wishlist_adds = int(totals.get('total_wishlist_adds', 0)) if totals else 0
    st.metric("❤️ Wishlist Adds", f"{wishlist_adds:,}")
with m_col4:
    total_orders = int(totals.get('total_orders', 0)) if totals else 0
    st.metric("📦 Total Orders", f"{total_orders:,}")
with m_col5:
    revenue = float(totals.get('total_revenue', 0)) if totals else 0.0
    st.metric("💰 Total Revenue", f"${revenue:,.2f}")

st.markdown("---")

# --- Charts: Time-Series Data ---
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("📊 Minutely Views & Orders")
    minutely_views_data = fetch_data("views:minutely")
    df_views = create_timeseries_df(minutely_views_data, "Views")

    if not df_views.empty:
        fig_views = px.bar(
            df_views, x='Time', y='Views',
            title='Product Views Over Time (Per Minute)',
            labels={'Views': 'Number of Views', 'Time': 'Time'},
            color='Views',
            color_continuous_scale='Blues'
        )
        fig_views.update_layout(
            title_x=0.5,
            font_family="sans-serif",
            showlegend=False
        )
        st.plotly_chart(fig_views, use_container_width=True)
    else:
        st.plotly_chart(create_empty_chart('Product Views Over Time (Per Minute)'), use_container_width=True)

with chart_col2:
    st.subheader("💰 Minutely Revenue")
    minutely_revenue_data = fetch_data("revenue:minutely")
    df_revenue = create_timeseries_df(minutely_revenue_data, "Revenue")

    if not df_revenue.empty:
        fig_revenue = px.line(
            df_revenue, x='Time', y='Revenue',
            title='Revenue Over Time (Per Minute)',
            markers=True,
            labels={'Revenue': 'Revenue ($)', 'Time': 'Time'}
        )
        fig_revenue.update_traces(
            line_color='#28a745',
            fill='tozeroy',
            fillcolor='rgba(40, 167, 69, 0.1)'
        )
        fig_revenue.update_layout(title_x=0.5, font_family="sans-serif")
        st.plotly_chart(fig_revenue, use_container_width=True)
    else:
        st.plotly_chart(create_empty_chart('Revenue Over Time (Per Minute)'), use_container_width=True)

st.markdown("---")

# --- Leaderboards: Top Products ---
top_prod_col1, top_prod_col2 = st.columns(2)

with top_prod_col1:
    st.subheader("🚀 Top 10 Viewed Products")
    top_viewed_data = fetch_data("product:views")
    df_top_viewed = create_leaderboard_df(top_viewed_data, "Views")

    if not df_top_viewed.empty:
        # Add ranking column
        df_top_viewed.insert(0, 'Rank', range(1, len(df_top_viewed) + 1))
        st.dataframe(
            df_top_viewed.head(10),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No product view data available yet.")

with top_prod_col2:
    st.subheader("📈 Top 10 Ordered Products")
    top_ordered_data = fetch_data("product:orders")
    df_top_ordered = create_leaderboard_df(top_ordered_data, "Orders")

    if not df_top_ordered.empty:
        # Add ranking column
        df_top_ordered.insert(0, 'Rank', range(1, len(df_top_ordered) + 1))
        st.dataframe(
            df_top_ordered.head(10),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No product order data available yet.")

st.markdown("---")

# --- Live Feeds: Recent Activity ---
activity_col1, activity_col2 = st.columns(2)

with activity_col1:
    st.subheader("🔥 Recent Orders")
    recent_orders_raw = fetch_data("recent:orders", 'list')
    if recent_orders_raw:
        st.info(f"📊 Showing {min(10, len(recent_orders_raw))} most recent orders")
        for i, order_json in enumerate(recent_orders_raw[:10]):
            try:
                order = json.loads(order_json)
                with st.container():
                    st.markdown(f"""
                        **Order #{i+1}:** `{order['order_id']}`
                        💰 **Amount:** `${order['total_amount']}`
                        📦 **Items:** `{order['item_count']}`
                    """)
                    st.markdown("---")
            except (json.JSONDecodeError, KeyError):
                continue
    else:
        st.info("🔍 No recent orders to display. Waiting for data...")

with activity_col2:
    st.subheader("👀 Recent Product Views")
    recent_views_raw = fetch_data("recent:views", 'list')
    if recent_views_raw:
        st.info(f"📊 Showing {min(10, len(recent_views_raw))} most recent views")
        for i, view_json in enumerate(recent_views_raw[:10]):
            try:
                view = json.loads(view_json)
                with st.container():
                    st.markdown(f"""
                        **View #{i+1}:** 👤 **User:** `{view['user_id']}`
                        🛍️ **Product:** `{view['product_name']}`
                    """)
                    st.markdown("---")
            except (json.JSONDecodeError, KeyError):
                continue
    else:
        st.info("🔍 No recent views to display. Waiting for data...")

# --- Status Footer ---
st.markdown("---")
footer_col1, footer_col2 = st.columns(2)
with footer_col1:
    st.caption("🔄 **Dashboard Status:** Real-time monitoring active")
with footer_col2:
    st.caption(f"🕒 **Last Refreshed:** {datetime.fromtimestamp(st.session_state.last_refresh_time).strftime('%H:%M:%S')}")

# Auto-refresh mechanism
if auto_refresh:
    # Use a small sleep to prevent excessive CPU usage
    time.sleep(0.1)
    # Set up the next refresh cycle
    if time_since_last_refresh < refresh_rate:
        time.sleep(min(1, refresh_rate - time_since_last_refresh))
        st.rerun()
