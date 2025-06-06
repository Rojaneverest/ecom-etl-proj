import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

# List of ODS tables to drop
ODS_TABLES = [
    "STG_GEOLOCATION",
    "STG_CUSTOMERS", 
    "STG_SELLERS",
    "STG_ORDERS",
    "STG_PRODUCTS",
    "STG_ORDER_ITEMS",
    "STG_ORDER_PAYMENTS",
    "STG_ORDER_REVIEWS",
    "STG_PRODUCT_CATEGORY_TRANSLATION"
]

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    role=SNOWFLAKE_ROLE
)

try:
    cur = conn.cursor()
    
    # Use the ECOM_ODS database
    cur.execute("USE DATABASE ECOM_STG;")
    cur.execute("USE SCHEMA PUBLIC;")
    print("📍 Connected to ECOM_STG.PUBLIC schema")
    
    # Drop each table
    dropped_count = 0
    for table_name in ODS_TABLES:
        try:
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")
            print(f"✅ Table '{table_name}' dropped successfully.")
            dropped_count += 1
        except Exception as table_error:
            print(f"❌ Error dropping table '{table_name}': {table_error}")
    
    print(f"\n🎯 Summary: {dropped_count}/{len(ODS_TABLES)} tables dropped successfully.")
    
except Exception as e:
    print(f"❌ Error occurred: {e}")
finally:
    cur.close()
    conn.close()
    print("🔌 Connection closed.")