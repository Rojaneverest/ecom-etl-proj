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
st.subheader("Live Metrics Overview")
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
st.subheader("📈 Minutely Activity")
chart_col1, chart_col2, chart_col3 = st.columns(3)

# Minutely Views
with chart_col1:
    minutely_views_data = fetch_data("views:minutely")
    df_views = create_timeseries_df(minutely_views_data, "Views")
    if not df_views.empty:
        fig = px.bar(df_views, x='Time', y='Views', title='Views per Minute', color='Views', color_continuous_scale='Blues')
        fig.update_layout(title_x=0.5, font_family="sans-serif", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.plotly_chart(create_empty_chart('Views per Minute'), use_container_width=True)

# Minutely Cart Adds
with chart_col2:
    minutely_cart_data = fetch_data("cart:minutely")
    df_cart = create_timeseries_df(minutely_cart_data, "Cart Adds")
    if not df_cart.empty:
        fig = px.bar(df_cart, x='Time', y='Cart Adds', title='Cart Adds per Minute', color='Cart Adds', color_continuous_scale='Greens')
        fig.update_layout(title_x=0.5, font_family="sans-serif", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.plotly_chart(create_empty_chart('Cart Adds per Minute'), use_container_width=True)

# Minutely Wishlist Adds
with chart_col3:
    minutely_wishlist_data = fetch_data("wishlist:minutely")
    df_wishlist = create_timeseries_df(minutely_wishlist_data, "Wishlist Adds")
    if not df_wishlist.empty:
        fig = px.bar(df_wishlist, x='Time', y='Wishlist Adds', title='Wishlist Adds per Minute', color='Wishlist Adds', color_continuous_scale='Reds')
        fig.update_layout(title_x=0.5, font_family="sans-serif", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.plotly_chart(create_empty_chart('Wishlist Adds per Minute'), use_container_width=True)

# --- Charts: Financial Time-Series ---
st.subheader("💰 Minutely Financials")
fin_chart_col1, fin_chart_col2, fin_chart_col3 = st.columns(3)

# Minutely Orders
with fin_chart_col1:
    minutely_orders_data = fetch_data("orders:minutely")
    df_orders = create_timeseries_df(minutely_orders_data, "Orders")
    if not df_orders.empty:
        fig = px.line(df_orders, x='Time', y='Orders', title='Orders per Minute', markers=True)
        fig.update_traces(line_color='#ff7f0e')
        fig.update_layout(title_x=0.5, font_family="sans-serif")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.plotly_chart(create_empty_chart('Orders per Minute'), use_container_width=True)

# Minutely Revenue
with fin_chart_col2:
    minutely_revenue_data = fetch_data("revenue:minutely")
    df_revenue = create_timeseries_df(minutely_revenue_data, "Revenue")
    if not df_revenue.empty:
        fig = px.line(df_revenue, x='Time', y='Revenue', title='Revenue per Minute', markers=True)
        fig.update_traces(line_color='#28a745', fill='tozeroy', fillcolor='rgba(40, 167, 69, 0.1)')
        fig.update_layout(title_x=0.5, font_family="sans-serif")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.plotly_chart(create_empty_chart('Revenue per Minute'), use_container_width=True)
        
# Minutely Cart Value
with fin_chart_col3:
    minutely_cart_value_data = fetch_data("cart:value:minutely")
    df_cart_value = create_timeseries_df(minutely_cart_value_data, "Cart Value")
    if not df_cart_value.empty:
        fig = px.line(df_cart_value, x='Time', y='Cart Value', title='Cart Value Added per Minute', markers=True)
        fig.update_traces(line_color='#17a2b8', fill='tozeroy', fillcolor='rgba(23, 162, 184, 0.1)')
        fig.update_layout(title_x=0.5, font_family="sans-serif")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.plotly_chart(create_empty_chart('Cart Value per Minute'), use_container_width=True)


st.markdown("---")

# --- Leaderboards: Top Products ---
st.subheader("🏆 Product Leaderboards")
lb_col1, lb_col2, lb_col3, lb_col4 = st.columns(4)

def display_leaderboard(df, title):
    if not df.empty:
        df.insert(0, 'Rank', range(1, len(df) + 1))
        st.dataframe(df.head(10), use_container_width=True, hide_index=True)
    else:
        st.info(f"No data for '{title}' yet.")

with lb_col1:
    st.markdown("##### Top Viewed")
    top_viewed_data = fetch_data("product:views")
    df_top_viewed = create_leaderboard_df(top_viewed_data, "Views")
    display_leaderboard(df_top_viewed, "Top Viewed")

with lb_col2:
    st.markdown("##### Top Added to Cart")
    top_cart_data = fetch_data("product:cart_adds")
    df_top_cart = create_leaderboard_df(top_cart_data, "Cart Adds")
    display_leaderboard(df_top_cart, "Top Added to Cart")
    
with lb_col3:
    st.markdown("##### Top Added to Wishlist")
    top_wishlist_data = fetch_data("product:wishlist_adds")
    df_top_wishlist = create_leaderboard_df(top_wishlist_data, "Wishlist Adds")
    display_leaderboard(df_top_wishlist, "Top Added to Wishlist")

with lb_col4:
    st.markdown("##### Top Ordered")
    top_ordered_data = fetch_data("product:orders")
    df_top_ordered = create_leaderboard_df(top_ordered_data, "Orders")
    display_leaderboard(df_top_ordered, "Top Ordered")

st.markdown("---")

# --- Categorical & Payment Breakdowns ---
st.subheader("📊 Categorical Breakdowns")
cat_col1, cat_col2, cat_col3 = st.columns(3)

with cat_col1:
    st.markdown("##### Views by Category")
    category_views_data = fetch_data("category:views")
    if category_views_data:
        df = create_leaderboard_df(category_views_data, "Views", name_col="Category").sort_values("Views", ascending=True)
        fig = px.bar(df, y="Category", x="Views", orientation='h', title="Views by Product Category")
        fig.update_layout(title_x=0.5, yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No category view data yet.")

with cat_col2:
    st.markdown("##### Revenue by Category")
    category_revenue_data = fetch_data("category:revenue")
    if category_revenue_data:
        df = create_leaderboard_df(category_revenue_data, "Revenue", name_col="Category")
        fig = px.pie(df, names="Category", values="Revenue", title="Revenue Share by Category", hole=0.3)
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(showlegend=False, title_x=0.5)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No category revenue data yet.")
        
with cat_col3:
    st.markdown("##### Payment Method Usage")
    payment_methods_data = fetch_data("payment:methods")
    if payment_methods_data:
        df = create_leaderboard_df(payment_methods_data, "Count", name_col="Payment Method")
        fig = px.pie(df, names="Payment Method", values="Count", title="Payment Method Distribution")
        fig.update_layout(showlegend=True, title_x=0.5)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No payment method data yet.")


st.markdown("---")

# --- Live Feeds: Recent Activity ---
st.subheader("🛰️ Live Activity Feed")
feed_col1, feed_col2, feed_col3, feed_col4 = st.columns(4)

with feed_col1:
    st.markdown("##### Recent Views")
    recent_views_raw = fetch_data("recent:views", 'list')
    if recent_views_raw:
        for i, view_json in enumerate(recent_views_raw[:10]):
            try:
                view = json.loads(view_json)
                st.info(f"👤 {view['user_id']} viewed **{view['product_name']}**")
            except (json.JSONDecodeError, KeyError): continue
    else:
        st.info("No recent views...")
        
with feed_col2:
    st.markdown("##### Recent Cart Adds")
    recent_cart_raw = fetch_data("recent:cart_adds", 'list')
    if recent_cart_raw:
        for i, cart_json in enumerate(recent_cart_raw[:10]):
            try:
                cart = json.loads(cart_json)
                st.success(f"🛒 {cart['user_id']} added **{cart['product_name']}** to cart.")
            except (json.JSONDecodeError, KeyError): continue
    else:
        st.info("No recent cart adds...")
        
with feed_col3:
    st.markdown("##### Recent Wishlist Adds")
    recent_wishlist_raw = fetch_data("recent:wishlist_adds", 'list')
    if recent_wishlist_raw:
        for i, wishlist_json in enumerate(recent_wishlist_raw[:10]):
            try:
                wishlist = json.loads(wishlist_json)
                st.warning(f"❤️ {wishlist['user_id']} wants **{wishlist['product_name']}**.")
            except (json.JSONDecodeError, KeyError): continue
    else:
        st.info("No recent wishlist adds...")

with feed_col4:
    st.markdown("##### Recent Orders")
    recent_orders_raw = fetch_data("recent:orders", 'list')
    if recent_orders_raw:
        for i, order_json in enumerate(recent_orders_raw[:10]):
            try:
                order = json.loads(order_json)
                st.error(f"💰 **${order['total_amount']:.2f}** order by {order['user_id']} via `{order['payment_method']}`")
            except (json.JSONDecodeError, KeyError): continue
    else:
        st.info("No recent orders...")

# --- Status Footer ---
st.markdown("---")
footer_col1, footer_col2 = st.columns(2)
with footer_col1:
    st.caption("🔄 **Dashboard Status:** Real-time monitoring active")
with footer_col2:
    st.caption(f"🕒 **Last Refreshed:** {datetime.fromtimestamp(st.session_state.last_refresh_time).strftime('%H:%M:%S')}")

# Auto-rerun logic
if auto_refresh:
    # Use a small sleep to prevent excessive CPU usage
    time.sleep(0.1)
    # Set up the next refresh cycle
    if time_since_last_refresh < refresh_rate:
        time.sleep(min(1, refresh_rate - time_since_last_refresh))
        st.rerun()
