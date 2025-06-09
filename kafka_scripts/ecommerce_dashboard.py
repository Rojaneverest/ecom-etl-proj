import streamlit as st
import redis
import pandas as pd
import time
import json
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
import os
from dotenv import load_dotenv

# --- Page Configuration ---
st.set_page_config(
    page_title="Real-Time E-commerce Dashboard",
    page_icon="🛒",
    layout="wide",
)

# --- Redis Connection ---
load_dotenv()

def get_redis_connection():
    """Establishes a connection to the Redis server."""
    try:
        redis_host = os.getenv('REDIS_HOST', 'redis')
        redis_port = int(os.getenv('REDIS_PORT', 6379))
        redis_db = int(os.getenv('REDIS_DB', 0))

        r = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )

        if r.ping():
            return r, None
        else:
            return None, "No response from server"

    except redis.exceptions.ConnectionError as e:
        return None, str(e)
    except Exception as e:
        return None, f"Unexpected error: {str(e)}"

# Initialize Redis connection with status handling
st.sidebar.title("🔌 Connection Status")
status_placeholder = st.sidebar.empty()

# Initialize session state for Redis if it doesn't exist
if 'redis_client' not in st.session_state:
    st.session_state.redis_initialized = False
    st.session_state.redis_client = None
    st.session_state.redis_error = None

# Check if we need to (re)initialize Redis connection
if not st.session_state.redis_initialized or st.session_state.redis_client is None:
    try:
        redis_client, error_message = get_redis_connection()
        if redis_client:
            st.session_state.redis_client = redis_client
            st.session_state.redis_error = None
            status_placeholder.success("✅ Connected to Redis")
        else:
            st.session_state.redis_error = error_message
            status_placeholder.error(f"❌ Redis: {error_message}")
            st.sidebar.error("Please ensure the Redis container is running and accessible.")
    except Exception as e:
        st.session_state.redis_error = str(e)
        status_placeholder.error(f"❌ Redis Error: {str(e)}")
        st.sidebar.error("Failed to connect to Redis. Please check the logs.")

    st.session_state.redis_initialized = True

# Show connection status
if st.session_state.redis_client:
    try:
        # Test the connection
        if st.session_state.redis_client.ping():
            status_placeholder.success("✅ Connected to Redis")
    except:
        # If ping fails, mark as disconnected
        status_placeholder.error("❌ Redis connection lost")
        st.session_state.redis_client = None
        st.sidebar.warning("Lost connection to Redis. Attempting to reconnect...")
        st.experimental_rerun()
else:
    status_placeholder.error(f"❌ Redis: {st.session_state.redis_error or 'Not connected'}")
    st.sidebar.error("Cannot connect to Redis. Some features may be unavailable.")
    st.stop()  # Stop execution if Redis is not connected

# --- Helper Functions ---
def fetch_data(redis_key, data_type='hash'):
    """Fetches data from Redis, gracefully handling connection or key errors."""
    if not st.session_state.get('redis_client'):
        st.warning("Not connected to Redis. Please check the connection.")
        return None
        
    try:
        if data_type == 'hash':
            data = st.session_state.redis_client.hgetall(redis_key)
        elif data_type == 'list':
            data = st.session_state.redis_client.lrange(redis_key, 0, 99)
        elif data_type == 'value':
            data = st.session_state.redis_client.get(redis_key)
        else:
            st.warning(f"Unsupported data type: {data_type}")
            return None
            
        # If we got None but no exception, the key might not exist yet
        if data is None:
            return None
            
        return data
        
    except redis.exceptions.ConnectionError as e:
        st.error("Lost connection to Redis. Attempting to reconnect...")
        st.session_state.redis_client = None
        st.experimental_rerun()
        return None
        
    except redis.exceptions.RedisError as e:
        st.warning(f"Could not fetch Redis key '{redis_key}': {e}")
        return None
        
    except Exception as e:
        st.error(f"Unexpected error fetching data: {e}")
        return None

def create_timeseries_df(data, value_col, time_col='Time'):
    """Converts minutely data from a Redis hash into a clean DataFrame for plotting.
    Assumes Redis keys are in YYYY-MM-DD-HH-MM format.
    """
    if not data:
        return pd.DataFrame({time_col: [], value_col: []})

    df = pd.DataFrame(list(data.items()), columns=['MinuteStr', value_col])
    # Updated format to include minutes
    df[time_col] = pd.to_datetime(df['MinuteStr'], format='%Y-%m-%d-%H-%M', errors='coerce')
    df[value_col] = pd.to_numeric(df[value_col], errors='coerce')
    df.dropna(inplace=True)
    return df.sort_values(time_col)

def create_leaderboard_df(data, value_col, name_col='Product ID'):
    """Converts a Redis hash into a sorted DataFrame for leaderboards."""
    if not data:
        return pd.DataFrame({name_col: [], value_col: []})

    df = pd.DataFrame(list(data.items()), columns=[name_col, value_col])
    df[value_col] = pd.to_numeric(df[value_col], errors='coerce')
    return df.sort_values(value_col, ascending=False).reset_index(drop=True)

def create_empty_chart(title):
    """Creates a placeholder chart with a message."""
    fig = go.Figure()
    fig.update_layout(
        title=title,
        xaxis={'visible': False},
        yaxis={'visible': False},
        annotations=[{
            "text": "Waiting for data...",
            "xref": "paper",
            "yref": "paper",
            "showarrow": False,
            "font": {"size": 20, "color": "gray"}
        }]
    )
    return fig

# --- Main Dashboard UI ---
st.title("📊 Real-Time E-commerce Analytics Dashboard")

# Stop execution if Redis is not connected
if not st.session_state.redis_client:
    st.stop()

# --- Auto-refresh Controls ---
col1, col2 = st.columns([0.8, 0.2])
with col1:
    # Use key to persist the slider value across reruns
    refresh_rate = st.slider("Select refresh rate (seconds)", 5, 60, 10, key="refresh_rate_slider")
with col2:
    st.write("") # for alignment
    st.write("") # for alignment
    # If the button is clicked, manually trigger a rerun
    if st.button("Refresh Now"):
        st.rerun() # This will cause the script to re-execute from the top

# Auto-refresh mechanism using st.experimental_rerun() with session state
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()

# Calculate time since last refresh
current_time = time.time()
time_since_refresh = current_time - st.session_state.last_refresh

# If refresh interval has passed, update the last refresh time and rerun
if time_since_refresh >= refresh_rate:
    st.session_state.last_refresh = current_time
    st.experimental_rerun()

# Add a small delay to prevent high CPU usage
time.sleep(0.1)


# --- Key Metrics Display ---
st.subheader("Live Metrics Overview")
totals = fetch_data("metrics:totals")
last_updated = totals.get('last_updated', 'Never') if totals else 'Never'

# Format last updated time for display
try:
    last_updated_dt = datetime.fromisoformat(last_updated)
    last_updated_str = last_updated_dt.strftime('%Y-%m-%d %H:%M:%S')
except (ValueError, TypeError):
    last_updated_str = 'N/A'

st.caption(f"Last data received: **{last_updated_str}**")

# Display metric cards
m_col1, m_col2, m_col3, m_col4, m_col5 = st.columns(5)
with m_col1:
    st.metric("👀 Total Views", f"{int(totals.get('total_views', 0)):,}" if totals else 0)
with m_col2:
    st.metric("🛒 Total Cart Adds", f"{int(totals.get('total_cart_adds', 0)):,}" if totals else 0)
with m_col3:
    st.metric("❤️ Wishlist Adds", f"{int(totals.get('total_wishlist_adds', 0)):,}" if totals else 0)
with m_col4:
    st.metric("📦 Total Orders", f"{int(totals.get('total_orders', 0)):,}" if totals else 0)
with m_col5:
    revenue = float(totals.get('total_revenue', 0)) if totals else 0.0
    st.metric("💰 Total Revenue", f"${revenue:,.2f}")

st.markdown("---")

# --- Charts: Time-Series Data ---
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("Minutely Views & Orders") # Changed to Minutely
    # Assuming new Redis key for minutely views
    minutely_views_data = fetch_data("views:minutely")
    df_views = create_timeseries_df(minutely_views_data, "Views")

    if not df_views.empty:
        fig_views = px.bar(
            df_views, x='Time', y='Views', title='Product Views Over Time (Minutely)', # Changed title
            labels={'Views': 'Number of Views', 'Time': 'Minute'} # Changed label
        )
        fig_views.update_layout(title_x=0.5, font_family="sans-serif")
        st.plotly_chart(fig_views, use_container_width=True)
    else:
        st.plotly_chart(create_empty_chart('Product Views Over Time (Minutely)'), use_container_width=True)

with chart_col2:
    st.subheader("Minutely Revenue") # Changed to Minutely
    # Assuming new Redis key for minutely revenue
    minutely_revenue_data = fetch_data("revenue:minutely")
    df_revenue = create_timeseries_df(minutely_revenue_data, "Revenue")

    if not df_revenue.empty:
        fig_revenue = px.line(
            df_revenue, x='Time', y='Revenue', title='Revenue Over Time (Minutely)', markers=True, # Changed title
            labels={'Revenue': 'Revenue ($)', 'Time': 'Minute'} # Changed label
        )
        fig_revenue.update_traces(line_color='#28a745', fill='tozeroy')
        fig_revenue.update_layout(title_x=0.5, font_family="sans-serif")
        st.plotly_chart(fig_revenue, use_container_width=True)
    else:
        st.plotly_chart(create_empty_chart('Revenue Over Time (Minutely)'), use_container_width=True)

st.markdown("---")

# --- Leaderboards: Top Products ---
top_prod_col1, top_prod_col2 = st.columns(2)

with top_prod_col1:
    st.subheader("🚀 Top 10 Viewed Products")
    top_viewed_data = fetch_data("product:views")
    df_top_viewed = create_leaderboard_df(top_viewed_data, "Views")
    st.dataframe(df_top_viewed.head(10), use_container_width=True)

with top_prod_col2:
    st.subheader("📈 Top 10 Ordered Products")
    top_ordered_data = fetch_data("product:orders")
    df_top_ordered = create_leaderboard_df(top_ordered_data, "Orders")
    st.dataframe(df_top_ordered.head(10), use_container_width=True)

st.markdown("---")

# --- Live Feeds: Recent Activity ---
activity_col1, activity_col2 = st.columns(2)

with activity_col1:
    st.subheader("🔥 Recent Orders")
    recent_orders_raw = fetch_data("recent:orders", 'list')
    if recent_orders_raw:
        with st.expander("View recent orders", expanded=True):
            for order_json in recent_orders_raw[:10]:
                try:
                    order = json.loads(order_json)
                    st.info(f"**Order:** `{order['order_id']}` | **Amount:** `${order['total_amount']}` | **Items:** `{order['item_count']}`")
                except json.JSONDecodeError:
                    continue
    else:
        st.write("No recent orders to display.")

with activity_col2:
    st.subheader("👀 Recent Product Views")
    recent_views_raw = fetch_data("recent:views", 'list')
    if recent_views_raw:
        with st.expander("View recent product views", expanded=True):
            for view_json in recent_views_raw[:10]:
                try:
                    view = json.loads(view_json)
                    st.text(f"User '{view['user_id']}' viewed '{view['product_name']}'")
                except json.JSONDecodeError:
                    continue
    else:
        st.write("No recent views to display.")
