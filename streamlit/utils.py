import os
import streamlit as st
import redis
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import json
import snowflake.connector
from dotenv import load_dotenv
from datetime import timedelta


# Load environment variables
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
            database=os.getenv("SNOWFLAKE_DWH_DB"),
            schema=os.getenv("SNOWFLAKE_DWH_SCHEMA")
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None
# --- NEW: Utility Function to Check for Table Existence ---
@st.cache_data(ttl=600)  # Cache for 10 mins to avoid re-checking on every script rerun
def check_tables_exist(_conn, required_tables):
    """
    Checks if a list of required tables exists in the connected Snowflake schema.

    Args:
        _conn: Active Snowflake connection object.
        required_tables (list): A list of table names to check (case-insensitive).

    Returns:
        list: A list of table names that are missing. Returns an empty list if all tables exist.
    """
    if not required_tables:
        return []

    try:
        db = _conn.database
        schema = _conn.schema
        
        # Snowflake stores identifiers in uppercase by default.
        tables_to_check_str = ", ".join([f"'{table.upper()}'" for table in required_tables])
        
        query = f"""
            SELECT TABLE_NAME 
            FROM {db}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{schema.upper()}'
            AND TABLE_NAME IN ({tables_to_check_str})
        """
        
        cursor = _conn.cursor()
        cursor.execute(query)
        
        found_tables = {row[0].upper() for row in cursor.fetchall()}
        cursor.close()
        
        required_tables_set = {table.upper() for table in required_tables}
        missing_tables = list(required_tables_set - found_tables)
        
        return missing_tables
        
    except Exception as e:
        st.error(f"An error occurred while verifying tables in Snowflake: {e}")
        # If the check fails, assume tables are missing to prevent further errors.
        return required_tables

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

# Snowflake data loading functions
@st.cache_data(ttl=3600)
def load_sales_overview_data(_conn):
    """Loads sales overview data from Snowflake."""
    DWH_DB = os.getenv("SNOWFLAKE_DWH_DB")
    DWH_SCHEMA = os.getenv("SNOWFLAKE_DWH_SCHEMA")

    # --- FIX 1: ADDED MISSING DATE COLUMNS TO THE QUERY ---
    query = f"""
        SELECT
            o.ORDER_ID, o.ORDER_PURCHASE_TIMESTAMP, o.ORDER_STATUS,
            o.ORDER_DELIVERED_CUSTOMER_DATE, o.ORDER_ESTIMATED_DELIVERY_DATE,
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
    df = cursor.fetch_pandas_all()
    cursor.close()

    # --- FIX 2: CONVERT ALL COLUMNS TO LOWERCASE TO PREVENT KEY ERRORS ---
    df.columns = [col.lower() for col in df.columns]
    
    return df


@st.cache_data(ttl=3600)
def load_daily_sales_data(_conn):
    """Loads daily sales data from Snowflake."""
    DWH_DB = os.getenv("SNOWFLAKE_DWH_DB")
    DWH_SCHEMA = os.getenv("SNOWFLAKE_DWH_SCHEMA")
    
    query = f"""
        SELECT
            DATE(o.ORDER_PURCHASE_TIMESTAMP) as "date",
            SUM(op.PAYMENT_VALUE) as "total_sales",
            COUNT(DISTINCT o.ORDER_ID) as "number_of_orders"
        FROM {DWH_DB}.{DWH_SCHEMA}.DWH_ORDERS AS o
        JOIN {DWH_DB}.{DWH_SCHEMA}.DWH_ORDER_PAYMENTS AS op ON o.ORDER_ID = op.ORDER_ID
        WHERE o.ORDER_STATUS NOT IN ('unavailable', 'canceled')
        GROUP BY 1 ORDER BY 1;
    """
    
    cursor = _conn.cursor()
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    cursor.close()
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    return df

@st.cache_data(ttl=3600)
def load_sales_by_geolocation(_conn):
    """Loads sales data by geolocation from Snowflake."""
    DWH_DB = os.getenv("SNOWFLAKE_DWH_DB")
    DWH_SCHEMA = os.getenv("SNOWFLAKE_DWH_SCHEMA")
    
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
    df = cursor.fetch_pandas_all()
    cursor.close()
    df.columns = [col.lower() for col in df.columns]
    return df

@st.cache_data
def process_delivery_and_satisfaction_data(df):
    """
    Processes the main sales DataFrame to calculate delivery metrics and handle datatypes.

    Args:
        df (pd.DataFrame): The main sales overview DataFrame from load_sales_overview_data.

    Returns:
        pd.DataFrame: The processed DataFrame with new columns for analysis.
    """
    # Create a copy to avoid modifying the cached original
    processed_df = df.copy()

    # Column names are now lowercase, so we use the lowercase versions here
    date_cols = [
        'order_purchase_timestamp',
        'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
    for col in date_cols:
        processed_df[col] = pd.to_datetime(processed_df[col], errors='coerce')

    # Drop rows where key dates are missing for delivery analysis
    processed_df.dropna(subset=date_cols, inplace=True)
    if processed_df.empty:
        return processed_df # Return empty df if no valid date rows exist
        
    # Calculate delivery metrics in days
    processed_df['actual_delivery_time'] = \
        (processed_df['order_delivered_customer_date'] - processed_df['order_purchase_timestamp']).dt.days
    
    processed_df['estimated_delivery_time'] = \
        (processed_df['order_estimated_delivery_date'] - processed_df['order_purchase_timestamp']).dt.days

    # Calculate the difference between estimated and actual delivery
    processed_df['delivery_delta_days'] = \
        processed_df['actual_delivery_time'] - processed_df['estimated_delivery_time']

    # Determine if the order was on time
    processed_df['delivery_status'] = processed_df.apply(
        lambda row: 'On-Time' if row['order_delivered_customer_date'] <= row['order_estimated_delivery_date'] else 'Late',
        axis=1
    )

    # Ensure review score is numeric
    processed_df['review_score'] = pd.to_numeric(processed_df['review_score'], errors='coerce')
    
    # Filter out invalid delivery time calculations (e.g., negative days)
    processed_df = processed_df[processed_df['actual_delivery_time'] >= 0]

    return processed_df

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

# --- Snowflake Data Loading Functions ---
@st.cache_data(ttl=3600)
def load_sales_overview_data(_conn):
    """Loads sales overview data from Snowflake."""
    DWH_DB = os.getenv("SNOWFLAKE_DWH_DB")
    DWH_SCHEMA = os.getenv("SNOWFLAKE_DWH_SCHEMA")

    query = f"""
        SELECT
            o.ORDER_ID, o.ORDER_PURCHASE_TIMESTAMP, o.ORDER_STATUS,
            o.ORDER_DELIVERED_CUSTOMER_DATE, o.ORDER_ESTIMATED_DELIVERY_DATE,
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
    df = cursor.fetch_pandas_all()
    cursor.close()

    df.columns = [col.lower() for col in df.columns]
    
    return df

@st.cache_data(ttl=3600)
def load_daily_sales_data(_conn):
    """Loads daily sales data from Snowflake."""
    # This function remains unchanged
    DWH_DB = os.getenv("SNOWFLAKE_DWH_DB")
    DWH_SCHEMA = os.getenv("SNOWFLAKE_DWH_SCHEMA")
    query = f"""
        SELECT DATE(o.ORDER_PURCHASE_TIMESTAMP) as "date", SUM(op.PAYMENT_VALUE) as "total_sales"
        FROM {DWH_DB}.{DWH_SCHEMA}.DWH_ORDERS AS o
        JOIN {DWH_DB}.{DWH_SCHEMA}.DWH_ORDER_PAYMENTS AS op ON o.ORDER_ID = op.ORDER_ID
        WHERE o.ORDER_STATUS NOT IN ('unavailable', 'canceled')
        GROUP BY 1 ORDER BY 1;
    """
    cursor = _conn.cursor()
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    cursor.close()
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    return df

@st.cache_data(ttl=3600)
def load_sales_by_geolocation(_conn):
    """Loads sales data by geolocation from Snowflake."""
    # This function remains unchanged
    DWH_DB = os.getenv("SNOWFLAKE_DWH_DB")
    DWH_SCHEMA = os.getenv("SNOWFLAKE_DWH_SCHEMA")
    query = f"""
        WITH SalesByCity AS (
            SELECT c.CUSTOMER_CITY, c.CUSTOMER_STATE, SUM(op.PAYMENT_VALUE) as "Total Sales"
            FROM {DWH_DB}.{DWH_SCHEMA}.DWH_ORDERS AS o
            JOIN {DWH_DB}.{DWH_SCHEMA}.DWH_CUSTOMERS AS c ON o.CUSTOMER_ID = c.CUSTOMER_ID
            JOIN {DWH_DB}.{DWH_SCHEMA}.DWH_ORDER_PAYMENTS AS op ON o.ORDER_ID = op.ORDER_ID
            WHERE o.ORDER_STATUS NOT IN ('unavailable', 'canceled') GROUP BY 1, 2
        )
        SELECT s.CUSTOMER_STATE, SUM(s."Total Sales") as "total_sales",
               AVG(g.GEOLOCATION_LAT) as "latitude", AVG(g.GEOLOCATION_LNG) as "longitude"
        FROM SalesByCity s JOIN {DWH_DB}.{DWH_SCHEMA}.DWH_GEOLOCATION g ON s.CUSTOMER_CITY = g.GEOLOCATION_CITY
        GROUP BY 1 ORDER BY "total_sales" DESC;
    """
    cursor = _conn.cursor()
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    cursor.close()
    df.columns = [col.lower() for col in df.columns]
    return df

@st.cache_data
def process_delivery_and_satisfaction_data(df):
    """Processes the main sales DataFrame to calculate delivery metrics."""
    # This function remains largely unchanged
    processed_df = df.copy()
    date_cols = ['order_purchase_timestamp', 'order_delivered_customer_date', 'order_estimated_delivery_date']
    for col in date_cols:
        processed_df[col] = pd.to_datetime(processed_df[col], errors='coerce')
    processed_df.dropna(subset=date_cols, inplace=True)
    if processed_df.empty: return processed_df
    processed_df['actual_delivery_time'] = (processed_df['order_delivered_customer_date'] - processed_df['order_purchase_timestamp']).dt.days
    processed_df['estimated_delivery_time'] = (processed_df['order_estimated_delivery_date'] - processed_df['order_purchase_timestamp']).dt.days
    processed_df['delivery_delta_days'] = processed_df['actual_delivery_time'] - processed_df['estimated_delivery_time']
    processed_df['delivery_status'] = processed_df.apply(lambda row: 'On-Time' if row['order_delivered_customer_date'] <= row['order_estimated_delivery_date'] else 'Late', axis=1)
    processed_df['review_score'] = pd.to_numeric(processed_df['review_score'], errors='coerce')
    processed_df = processed_df[processed_df['actual_delivery_time'] >= 0]
    return processed_df

# --- NEW: RFM Analysis Function ---
@st.cache_data
def calculate_rfm(df):
    """
    Calculates Recency, Frequency, and Monetary (RFM) scores for each customer.
    This version is robust against duplicate values that cause qcut errors.
    """
    rfm_df = df.copy()

    # Ensure required columns are present and handle data types
    rfm_df['order_purchase_timestamp'] = pd.to_datetime(rfm_df['order_purchase_timestamp'], errors='coerce')
    rfm_df.dropna(subset=['order_purchase_timestamp', 'customer_unique_id', 'payment_value'], inplace=True)
    if rfm_df.empty:
        return pd.DataFrame()

    # Determine the snapshot date for recency calculation
    snapshot_date = rfm_df['order_purchase_timestamp'].max() + timedelta(days=1)
    
    # Calculate RFM metrics
    rfm = rfm_df.groupby('customer_unique_id').agg({
        'order_purchase_timestamp': lambda x: (snapshot_date - x.max()).days,
        'order_id': 'nunique',
        'payment_value': 'sum'
    })
    
    rfm.rename(columns={
        'order_purchase_timestamp': 'Recency',
        'order_id': 'Frequency',
        'payment_value': 'Monetary'
    }, inplace=True)

    # Define scoring labels
    r_labels = [4, 3, 2, 1] # Higher recency (fewer days) gets a higher score
    f_labels = [1, 2, 3, 4] # Higher frequency gets a higher score
    m_labels = [1, 2, 3, 4] # Higher monetary value gets a higher score

    # --- FIX: Add .rank(method='first') to handle duplicate values ---
    # This prevents the "ValueError" by ensuring each value has a unique rank before binning.
    rfm['R_Score'] = pd.qcut(rfm['Recency'].rank(method='first'), q=4, labels=r_labels).astype(int)
    rfm['F_Score'] = pd.qcut(rfm['Frequency'].rank(method='first'), q=4, labels=f_labels).astype(int)
    rfm['M_Score'] = pd.qcut(rfm['Monetary'].rank(method='first'), q=4, labels=m_labels).astype(int)

    # Define customer segments based on scores
    def rfm_segment(row):
        if row['R_Score'] >= 4 and row['F_Score'] >= 4:
            return 'Champions'
        if row['R_Score'] >= 3 and row['F_Score'] >= 3:
            return 'Loyal Customers'
        if row['R_Score'] >= 3 and row['F_Score'] < 3:
            return 'Potential Loyalists'
        if row['R_Score'] < 3 and row['F_Score'] >= 4:
            return 'Cannot Lose'
        if row['R_Score'] >= 2 and row['F_Score'] >= 2:
            return 'At Risk'
        if row['R_Score'] < 2 and row['F_Score'] < 2:
            return 'Hibernating'
        return 'Needs Attention'

    rfm['Segment'] = rfm.apply(rfm_segment, axis=1)

    return rfm.reset_index()


# --- NEW: Seller Performance Function ---
@st.cache_data
def calculate_seller_performance(df):
    """Calculates key performance indicators for each seller."""
    seller_df = df.dropna(subset=['seller_id', 'review_score', 'actual_delivery_time'])
    
    seller_performance = seller_df.groupby('seller_id').agg(
        total_revenue=('payment_value', 'sum'),
        total_orders=('order_id', 'nunique'),
        avg_review_score=('review_score', 'mean'),
        avg_delivery_time=('actual_delivery_time', 'mean')
    ).reset_index()

    # Round the metrics for cleaner display
    seller_performance['total_revenue'] = seller_performance['total_revenue'].round(2)
    seller_performance['avg_review_score'] = seller_performance['avg_review_score'].round(2)
    seller_performance['avg_delivery_time'] = seller_performance['avg_delivery_time'].round(1)
    
    return seller_performance
