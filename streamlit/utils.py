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
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None

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
    """.format(DWH_DB=DWH_DB, DWH_SCHEMA=DWH_SCHEMA)
    
    cursor = _conn.cursor()
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    cursor.close()
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
    """.format(DWH_DB=DWH_DB, DWH_SCHEMA=DWH_SCHEMA)
    
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
    """.format(DWH_DB=DWH_DB, DWH_SCHEMA=DWH_SCHEMA)
    
    cursor = _conn.cursor()
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    cursor.close()
    df.columns = [col.lower() for col in df.columns]
    return df
