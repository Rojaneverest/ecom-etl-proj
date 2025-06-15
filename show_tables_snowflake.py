import os
import sys
import snowflake.connector
from dotenv import load_dotenv
import logging

# --- Load environment variables ---
load_dotenv()

# --- Snowflake Connection Configuration ---
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_DWH_DB = os.getenv("SNOWFLAKE_DWH_DB", "ECOM_DWH")
SNOWFLAKE_DWH_SCHEMA = os.getenv("SNOWFLAKE_DWH_SCHEMA", "PUBLIC")

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("SnowflakeTableInspector")

def show_columns_and_sample_row(conn, db, schema):
    """Display all column names and one row of data from each table."""
    table_names = [
        "DWH_GEOLOCATION", "DWH_PRODUCTS", "DWH_CUSTOMERS", "DWH_SELLERS",
        "DWH_ORDERS", "DWH_ORDER_ITEMS", "DWH_ORDER_PAYMENTS", "DWH_ORDER_REVIEWS"
    ]

    for table in table_names:
        logger.info(f"\n--- Table: {table} ---")
        try:
            cursor = conn.cursor()

            # Describe table to get columns
            cursor.execute(f"DESCRIBE TABLE {db}.{schema}.{table}")
            columns = [row[0] for row in cursor.fetchall()]
            logger.info(f"Columns: {columns}")

            # Fetch 1 row of data
            cursor.execute(f"SELECT * FROM {db}.{schema}.{table} LIMIT 1")
            row = cursor.fetchone()
            if row:
                row_dict = dict(zip(columns, row))
                logger.info(f"Sample Row: {row_dict}")
            else:
                logger.info("Sample Row: No data available.")
        except Exception as e:
            logger.warning(f"Error accessing table {table}: {e}")

def main():
    logger.info("Connecting to Snowflake...")
    conn = None
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            role=SNOWFLAKE_ROLE,
            database=SNOWFLAKE_DWH_DB,
            schema=SNOWFLAKE_DWH_SCHEMA
        )
        logger.info("Connection successful.")

        show_columns_and_sample_row(conn, SNOWFLAKE_DWH_DB, SNOWFLAKE_DWH_SCHEMA)

    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            logger.info("Connection closed.")

if __name__ == "__main__":
    main()
