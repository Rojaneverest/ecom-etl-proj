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

# --- Additional Imports for Historical Analytics ---
import snowflake.connector
# The problematic import 'pd_fetch_pandas_all' has been removed.

# --- Page Configuration ---
st.set_page_config(
    page_title="E-commerce Analytics Dashboard",
    page_icon="🛒",
    layout="wide",
)

# --- Load Environment Variables ---
load_dotenv()

# --- Redis Connection ---
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
        )
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

# --- Snowflake Connection ---
@st.cache_resource
def get_snowflake_connection():
    """Establishes a connection to Snowflake and returns the connection object."""
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None

# --- Sidebar Navigation ---
st.sidebar.title("Dashboard Options")
selected_option = st.sidebar.radio(
    "Choose an analytics view:",
    ("Realtime Analytics", "Historical Analytics")
)

# --- REALTIME ANALYTICS SECTION ---
if selected_option == "Realtime Analytics":
    st.title("📊 Real-Time E-commerce Analytics Dashboard")

    # --- Auto-refresh Controls (LOGIC KEPT, UI REMOVED) ---

    # Set default values for refresh controls as they are no longer in the UI
    refresh_rate = 5 # Default auto-refresh interval in seconds
    auto_refresh_enabled = True # Auto-refresh is enabled by default
    manual_refresh = False # No manual refresh button in UI, so always False

    # Initialize session state for refresh control
    if 'last_refresh_time' not in st.session_state:
        st.session_state.last_refresh_time = time.time()

    # Auto-refresh logic
    current_time = time.time()
    time_since_last_refresh = current_time - st.session_state.last_refresh_time

    if auto_refresh_enabled and (time_since_last_refresh >= refresh_rate or manual_refresh):
        st.session_state.last_refresh_time = current_time
        time.sleep(0.5) # Brief pause (optional, for effect if any visual element was refreshed)
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
    footer_col1, footer_col2 = st.columns(2) # Reduced to 2 columns as refresh rate and last refresh time are now static
    with footer_col1:
        st.caption("🔄 **Dashboard Status:** Real-time monitoring active")
    with footer_col2:
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

# --- HISTORICAL ANALYTICS SECTION ---
elif selected_option == "Historical Analytics":
    st.title("🏛️ Historical E-commerce Performance")
    st.markdown("Analysis of aggregated data from the E-Commerce Data Warehouse.")

    # --- Snowflake Data Loading Functions for Historical Data ---
    DWH_DB = os.getenv("SNOWFLAKE_DWH_DB")
    DWH_SCHEMA = os.getenv("SNOWFLAKE_DWH_SCHEMA")

    @st.cache_data(ttl=3600)  # Cache data for 1 hour
    def load_sales_overview_data(_conn):
        query = f"""
            SELECT
                o.ORDER_ID, o.ORDER_PURCHASE_TIMESTAMP, o.ORDER_STATUS,
                oi.PRICE, oi.FREIGHT_VALUE,
                op.PAYMENT_VALUE, op.PAYMENT_TYPE, op.PAYMENT_INSTALLMENTS,
                p.PRODUCT_CATEGORY_NAME,
                c.CUSTOMER_UNIQUE_ID, c.CUSTOMER_CITY, c.CUSTOMER_STATE,
                s.SELLER_ID, s.SELLER_CITY, s.SELLER_STATE,
                r.REVIEW_SCORE
            FROM {DWH_DB}.{DWH_SCHEMA}.DWH_ORDERS AS o
            LEFT JOIN {DWH_DB}.{DWH_SCHEMA}.DWH_ORDER_ITEMS AS oi ON o.ORDER_ID = oi.ORDER_ID
            LEFT JOIN {DWH_DB}.{DWH_SCHEMA}.DWH_ORDER_PAYMENTS AS op ON o.ORDER_ID = op.ORDER_ID
            LEFT JOIN {DWH_DB}.{DWH_SCHEMA}.DWH_PRODUCTS AS p ON oi.PRODUCT_ID = p.PRODUCT_ID
            LEFT JOIN {DWH_DB}.{DWH_SCHEMA}.DWH_CUSTOMERS AS c ON o.CUSTOMER_ID = c.CUSTOMER_ID
            LEFT JOIN {DWH_DB}.{DWH_SCHEMA}.DWH_SELLERS AS s ON oi.SELLER_ID = s.SELLER_ID
            LEFT JOIN {DWH_DB}.{DWH_SCHEMA}.DWH_ORDER_REVIEWS AS r ON o.ORDER_ID = r.ORDER_ID
            WHERE o.ORDER_PURCHASE_TIMESTAMP IS NOT NULL AND oi.PRICE IS NOT NULL;
        """
        cursor = _conn.cursor()
        cursor.execute(query)
        # FIX: Changed to the modern method
        df = cursor.fetch_pandas_all()
        cursor.close()
        return df

    @st.cache_data(ttl=3600)
    def load_daily_sales_data(_conn):
        query = f"""
            SELECT
                DATE(o.ORDER_PURCHASE_TIMESTAMP) as "Date",
                SUM(op.PAYMENT_VALUE) as "Total Sales",
                COUNT(DISTINCT o.ORDER_ID) as "Number of Orders"
            FROM {DWH_DB}.{DWH_SCHEMA}.DWH_ORDERS AS o
            JOIN {DWH_DB}.{DWH_SCHEMA}.DWH_ORDER_PAYMENTS AS op ON o.ORDER_ID = op.ORDER_ID
            WHERE o.ORDER_STATUS NOT IN ('unavailable', 'canceled')
            GROUP BY 1 ORDER BY 1;
        """
        cursor = _conn.cursor()
        cursor.execute(query)
        # FIX: Changed to the modern method
        df = cursor.fetch_pandas_all()
        cursor.close()
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        return df

    @st.cache_data(ttl=3600)
    def load_sales_by_geolocation(_conn):
        query = f"""
            WITH SalesByCity AS (
                SELECT
                    c.CUSTOMER_CITY, c.CUSTOMER_STATE,
                    SUM(op.PAYMENT_VALUE) as "Total Sales"
                FROM {DWH_DB}.{DWH_SCHEMA}.DWH_ORDERS AS o
                JOIN {DWH_DB}.{DWH_SCHEMA}.DWH_CUSTOMERS AS c ON o.CUSTOMER_ID = c.CUSTOMER_ID
                JOIN {DWH_DB}.{DWH_SCHEMA}.DWH_ORDER_PAYMENTS AS op ON o.ORDER_ID = op.ORDER_ID
                WHERE o.ORDER_STATUS NOT IN ('unavailable', 'canceled')
                GROUP BY 1, 2
            )
            SELECT
                s.CUSTOMER_STATE,
                SUM(s."Total Sales") as "total_sales",
                AVG(g.GEOLOCATION_LAT) as "latitude",
                AVG(g.GEOLOCATION_LNG) as "longitude"
            FROM SalesByCity s
            JOIN {DWH_DB}.{DWH_SCHEMA}.DWH_GEOLOCATION g ON s.CUSTOMER_CITY = g.GEOLOCATION_CITY
            GROUP BY 1
            ORDER BY "total_sales" DESC;
        """
        cursor = _conn.cursor()
        cursor.execute(query)
        # FIX: Changed to the modern method
        df = cursor.fetch_pandas_all()
        cursor.close()
        # Make column names lowercase for easier use
        df.columns = [col.lower() for col in df.columns]
        return df

    # --- Load Data and Display Dashboard ---
    conn = get_snowflake_connection()
    if conn:
        with st.spinner("Loading historical data from Warehouse... ❄️"):
            sales_df = load_sales_overview_data(conn)
            daily_sales_df = load_daily_sales_data(conn)
            geo_sales_df = load_sales_by_geolocation(conn)

            # --- KPIs for Historical Data ---
            st.markdown("---")
            st.subheader("🚀 Overall Performance Metrics")

            total_revenue = sales_df['PAYMENT_VALUE'].sum()
            total_orders = sales_df['ORDER_ID'].nunique()
            unique_customers = sales_df['CUSTOMER_UNIQUE_ID'].nunique()
            avg_review_score = sales_df['REVIEW_SCORE'].mean()

            kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
            kpi_col1.metric("💰 Total Revenue", f"${total_revenue:,.2f}")
            kpi_col2.metric("📦 Total Orders", f"{total_orders:,}")
            kpi_col3.metric("👥 Unique Customers", f"{unique_customers:,}")
            kpi_col4.metric("⭐ Average Review Score", f"{avg_review_score:.2f}")

            st.markdown("---")

            # --- Time Series and Geo Charts ---
            chart_col1, chart_col2 = st.columns(2)
            with chart_col1:
                st.subheader("📅 Daily Sales Trend")
                fig_daily_sales = px.line(daily_sales_df, x='date', y='total_sales', title="Total Revenue Over Time")
                st.plotly_chart(fig_daily_sales, use_container_width=True)

            with chart_col2:
                st.subheader("🗺️ Sales by Customer State")
                # Ensure the 'size' column exists and is numeric
                geo_sales_df['total_sales'] = pd.to_numeric(geo_sales_df['total_sales'], errors='coerce')
                geo_sales_df.dropna(subset=['latitude', 'longitude', 'total_sales'], inplace=True)

                # Normalize 'total_sales' for better map visualization
                min_sales = geo_sales_df['total_sales'].min()
                max_sales = geo_sales_df['total_sales'].max()

                if max_sales > min_sales: # Avoid division by zero if all sales are the same
                    geo_sales_df['scaled_sales'] = (
                        (geo_sales_df['total_sales'] - min_sales) / (max_sales - min_sales)
                    ) * 95 + 5 # Scale to a range of 5 to 100 for marker size
                else:
                    geo_sales_df['scaled_sales'] = 5 # Default size if all sales are the same

                # Use st.container() to ensure proper width calculation
                with st.container():
                    st.map(
                        geo_sales_df, 
                        latitude='latitude', 
                        longitude='longitude', 
                        size='scaled_sales',
                        use_container_width=True
                    )

            # --- Leaderboards and Distributions ---
            lb_col1, lb_col2 = st.columns(2)
            with lb_col1:
                st.subheader("🛍️ Top 10 Product Categories by Revenue")
                category_sales = sales_df.groupby('PRODUCT_CATEGORY_NAME')['PAYMENT_VALUE'].sum().nlargest(10).reset_index()
                category_sales.columns = ['Product Category', 'Total Revenue']
                st.dataframe(category_sales, use_container_width=True, hide_index=True)

            with lb_col2:
                st.subheader("🏆 Top 10 Seller States by Revenue")
                seller_state_sales = sales_df.groupby('SELLER_STATE')['PAYMENT_VALUE'].sum().nlargest(10).reset_index()
                seller_state_sales.columns = ['Seller State', 'Total Revenue']
                st.dataframe(seller_state_sales, use_container_width=True, hide_index=True)

            st.markdown("---")

            # --- Payment Type Distribution ---
            st.subheader("💳 Payment Method Distribution")
            payment_dist = sales_df['PAYMENT_TYPE'].value_counts().reset_index()
            payment_dist.columns = ['Payment Type', 'Number of Transactions']
            fig_payment = px.pie(payment_dist, names='Payment Type', values='Number of Transactions',
                                 title='Popularity of Payment Methods', hole=0.3)
            st.plotly_chart(fig_payment, use_container_width=True)

    else:
        st.warning("Could not establish a connection to Snowflake. Please check your credentials in the `.env` file.")