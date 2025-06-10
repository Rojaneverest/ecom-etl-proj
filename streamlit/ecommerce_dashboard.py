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

@st.cache_resource
def get_redis_connection():
    """Establishes a connection to the Redis server with caching."""
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
            socket_timeout=5,
            retry_on_timeout=True,
            health_check_interval=30
        )

        # Test connection
        r.ping()
        return r

    except redis.exceptions.ConnectionError as e:
        st.error(f"Redis Connection Error: {str(e)}")
        return None
    except Exception as e:
        st.error(f"Unexpected Redis error: {str(e)}")
        return None

# Initialize Redis connection
redis_client = get_redis_connection()

# --- Sidebar Navigation ---
st.sidebar.title("Dashboard Options")
selected_option = st.sidebar.radio(
    "Choose an analytics view:",
    ("Realtime Analytics", "Historical Analytics")
)

# --- Main Dashboard UI (Conditional based on sidebar selection) ---
if selected_option == "Realtime Analytics":
    st.title("📊 Real-Time E-commerce Analytics Dashboard")

    # --- Auto-refresh Controls ---
    col1, col2, col3 = st.columns([0.6, 0.2, 0.2])
    with col1:
        refresh_rate = st.selectbox(
            "Auto-refresh interval:", 
            [10, 15, 30, 60], 
            index=0,
            format_func=lambda x: f"{x} seconds"
        )
    with col2:
        st.write("")  # spacing
        manual_refresh = st.button("🔄 Refresh Now", type="secondary")
    with col3:
        st.write("")  # spacing
        auto_refresh_enabled = st.checkbox("Auto-refresh", value=True)

    # Initialize session state for refresh control
    if 'last_refresh_time' not in st.session_state:
        st.session_state.last_refresh_time = time.time()

    # Auto-refresh logic
    current_time = time.time()
    time_since_last_refresh = current_time - st.session_state.last_refresh_time

    # Create a placeholder for the countdown
    countdown_placeholder = st.empty()

    if auto_refresh_enabled:
        time_to_next_refresh = max(0, refresh_rate - time_since_last_refresh)
        
        if time_to_next_refresh > 0:
            countdown_placeholder.info(f"⏱️ Next refresh in: {int(time_to_next_refresh)} seconds")
        
        # Trigger refresh when time is up or manual refresh is clicked
        if time_since_last_refresh >= refresh_rate or manual_refresh:
            st.session_state.last_refresh_time = current_time
            countdown_placeholder.success("🔄 Refreshing data...")
            time.sleep(0.5)  # Brief pause to show refresh message
            st.rerun()
    else:
        countdown_placeholder.info("⏸️ Auto-refresh disabled")
        if manual_refresh:
            st.session_state.last_refresh_time = current_time
            countdown_placeholder.success("🔄 Refreshing data...")
            time.sleep(0.5)
            st.rerun()

    # --- Helper Functions (defined inside the conditional block if they are only used for Realtime) ---
    def fetch_data(redis_key, data_type='hash'):
        """Fetches data from Redis, gracefully handling connection or key errors."""
        if not redis_client:
            return None
            
        try:
            if data_type == 'hash':
                data = redis_client.hgetall(redis_key)
            elif data_type == 'list':
                data = redis_client.lrange(redis_key, 0, 99)
            elif data_type == 'value':
                data = redis_client.get(redis_key)
            else:
                return None
                
            return data
            
        except redis.exceptions.RedisError as e:
            st.warning(f"Could not fetch Redis key '{redis_key}': {e}")
            return None
        except Exception as e:
            st.error(f"Unexpected error fetching data: {e}")
            return None

    def create_timeseries_df(data, value_col, time_col='Time'):
        """Converts minutely data from a Redis hash into a clean DataFrame for plotting."""
        if not data:
            return pd.DataFrame({time_col: [], value_col: []})

        df = pd.DataFrame(list(data.items()), columns=['MinuteStr', value_col])
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
    footer_col1, footer_col2, footer_col3 = st.columns(3)
    with footer_col1:
        st.caption("🔄 **Dashboard Status:** Real-time monitoring active")
    with footer_col2:
        st.caption(f"⚡ **Refresh Rate:** {refresh_rate} seconds")
    with footer_col3:
        st.caption(f"🕒 **Last Refreshed:** {datetime.fromtimestamp(st.session_state.last_refresh_time).strftime('%H:%M:%S')}")

    # Auto-refresh mechanism - this ensures the page keeps refreshing
    if auto_refresh_enabled:
        # Use a small sleep to prevent excessive CPU usage
        time.sleep(0.1)
        # Set up the next refresh cycle
        if time_since_last_refresh < refresh_rate:
            # Use st.empty() and time.sleep() for smoother refresh timing
            time.sleep(min(1, refresh_rate - time_since_last_refresh))
            st.rerun()

elif selected_option == "Historical Analytics":
    st.title("🏛️ Historical Analytics (Under Construction)")
    st.info("This section is dedicated to historical data analysis and will be implemented soon.")
    st.write("You can add your historical analytics logic here when you're ready.")