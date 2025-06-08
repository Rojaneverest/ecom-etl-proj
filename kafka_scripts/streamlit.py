import streamlit as st
import redis
import pandas as pd
import time
import json
from datetime import datetime
import plotly.express as px

# --- Page Configuration ---
st.set_page_config(
    page_title="Real-Time E-commerce Dashboard",
    page_icon="📊",
    layout="wide",
)

# --- Redis Connection ---
@st.cache_resource
def get_redis_connection():
    """Establishes a connection to the Redis server."""
    try:
        # In Docker, connect to the Redis service name
        r = redis.Redis(host='redis', port=6379, decode_responses=True)
        r.ping()
        st.success("✅ Connected to Redis!")
        return r
    except redis.ConnectionError as e:
        st.error(f"❌ Failed to connect to Redis: {e}")
        st.info("Please ensure Redis is running on localhost:6379 and that your consumer.py script is populating it with data.")
        return None

redis_client = get_redis_connection()

# --- Helper Functions ---
def fetch_data(redis_key, data_type='hash'):
    """Fetches data from Redis based on key and data type."""
    if not redis_client:
        return None
    try:
        if data_type == 'hash':
            return redis_client.hgetall(redis_key)
        elif data_type == 'list':
            return redis_client.lrange(redis_key, 0, -1)
        elif data_type == 'value':
            return redis_client.get(redis_key)
    except Exception as e:
        st.warning(f"Could not fetch data for key '{redis_key}': {e}")
    return None

def create_timeseries_df(data, value_col_name):
    """Converts hourly data from Redis into a Pandas DataFrame for plotting."""
    if not data:
        return pd.DataFrame(columns=['Time', value_col_name])

    df = pd.DataFrame(list(data.items()), columns=['HourStr', value_col_name])
    df['Time'] = pd.to_datetime(df['HourStr'], format='%Y-%m-%d-%H')
    df[value_col_name] = pd.to_numeric(df[value_col_name])
    return df.sort_values('Time')

def create_top_products_df(data, value_col_name):
    """Creates a DataFrame for top products."""
    if not data:
        return pd.DataFrame(columns=['Product ID', value_col_name])
    
    df = pd.DataFrame(list(data.items()), columns=['Product ID', value_col_name])
    df[value_col_name] = pd.to_numeric(df[value_col_name])
    return df.sort_values(value_col_name, ascending=False).reset_index(drop=True)

# --- Dashboard UI ---
st.title("📊 Real-Time E-commerce Dashboard")

if redis_client:
    # --- Auto-refresh control ---
    refresh_rate = st.slider("Select refresh rate (seconds)", 5, 60, 10)
    
    # Placeholder for the main content
    placeholder = st.empty()

    while True:
        with placeholder.container():
            # --- Key Metrics ---
            totals = fetch_data("metrics:totals")
            last_updated = totals.get('last_updated', 'N/A')
            
            st.markdown(f"**Last Updated:** `{last_updated}`")

            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("👀 Total Views", totals.get('total_views', 0))
            with col2:
                st.metric("🛒 Total Cart Adds", totals.get('total_cart_adds', 0))
            with col3:
                st.metric("❤️ Total Wishlist Adds", totals.get('total_wishlist_adds', 0))
            with col4:
                st.metric("📦 Total Orders", totals.get('total_orders', 0))
            with col5:
                st.metric("💰 Total Revenue", f"${float(totals.get('total_revenue', 0)):,.2f}")

            st.markdown("---")

            # --- Charts ---
            chart_col1, chart_col2 = st.columns(2)

            with chart_col1:
                st.subheader("Hourly Activity")
                # Views over time
                hourly_views_data = fetch_data("views:hourly")
                df_views = create_timeseries_df(hourly_views_data, "Views")
                fig_views = px.bar(df_views, x='Time', y='Views', title='Product Views Over Time')
                st.plotly_chart(fig_views, use_container_width=True)

            with chart_col2:
                st.subheader("Hourly Revenue & Orders")
                # Revenue over time
                hourly_revenue_data = fetch_data("revenue:hourly")
                df_revenue = create_timeseries_df(hourly_revenue_data, "Revenue")
                fig_revenue = px.line(df_revenue, x='Time', y='Revenue', title='Revenue Over Time', markers=True)
                fig_revenue.update_traces(line_color='#28a745')
                st.plotly_chart(fig_revenue, use_container_width=True)


            # --- Top Products ---
            top_prod_col1, top_prod_col2 = st.columns(2)

            with top_prod_col1:
                st.subheader("🚀 Top Viewed Products")
                top_viewed_data = fetch_data("product:views")
                df_top_viewed = create_top_products_df(top_viewed_data, "Views")
                st.dataframe(df_top_viewed.head(10), use_container_width=True)

            with top_prod_col2:
                st.subheader("📈 Top Ordered Products")
                top_ordered_data = fetch_data("product:orders")
                df_top_ordered = create_top_products_df(top_ordered_data, "Orders")
                st.dataframe(df_top_ordered.head(10), use_container_width=True)

            st.markdown("---")
            
            # --- Recent Activity Feeds ---
            activity_col1, activity_col2 = st.columns(2)

            with activity_col1:
                st.subheader("🔥 Recent Orders")
                recent_orders_raw = fetch_data("recent:orders", 'list')
                if recent_orders_raw:
                    recent_orders = [json.loads(o) for o in recent_orders_raw]
                    for order in recent_orders[:10]:
                        st.info(f"**Order ID:** `{order['order_id']}`\n\n**Amount:** `${order['total_amount']}` | **Items:** `{order['item_count']}`")
                else:
                    st.write("No recent orders.")
            
            with activity_col2:
                st.subheader("👀 Recent Product Views")
                recent_views_raw = fetch_data("recent:views", 'list')
                if recent_views_raw:
                    recent_views = [json.loads(v) for v in recent_views_raw]
                    for view in recent_views[:10]:
                        st.text(f"User '{view['user_id']}' viewed '{view['product_name']}'")
                else:
                    st.write("No recent views.")
        
        time.sleep(refresh_rate)
else:
    st.warning("Dashboard cannot be displayed. Waiting for Redis connection...")
