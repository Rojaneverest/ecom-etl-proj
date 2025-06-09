import os
import sys
import snowflake.connector
from dotenv import load_dotenv
import logging

# --- Load Environment Variables ---
load_dotenv()

# --- Configuration ---
# Snowflake Connection Configuration (using existing vars from your provided code)
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

# Target Data Warehouse Database/Schema
DWH_DB = os.getenv("SNOWFLAKE_DWH_DB", "ECOM_DWH")
DWH_SCHEMA = os.getenv("SNOWFLAKE_DWH_SCHEMA", "PUBLIC") # Assuming final tables are in PUBLIC schema

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("snowflake_datetime_checker")

# --- Core Functions ---

def create_snowflake_connection():
    """Establishes a connection to Snowflake and returns the connection object."""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            role=SNOWFLAKE_ROLE,
            database=DWH_DB,  # Connect directly to the DWH database
            schema=DWH_SCHEMA # And to the DWH schema
        )
        logger.info(f"Successfully connected to Snowflake, using DWH: {DWH_DB}.{DWH_SCHEMA}")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        sys.exit(1)

def fetch_datetime_columns(conn):
    """
    Fetches all datetime (TIMESTAMP, DATE, TIME) column names
    from tables in the specified Data Warehouse database and schema.

    Args:
        conn: A Snowflake connection object.

    Returns:
        dict: A dictionary where keys are table names and values are lists of datetime column names.
    """
    datetime_columns_by_table = {}

    # Query the INFORMATION_SCHEMA to get column details
    # We explicitly look for 'BASE TABLE' to exclude views.
    # The DATA_TYPE can be 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ', 'DATE', 'TIME'.
    query = f"""
    SELECT
        TABLE_NAME,
        COLUMN_NAME,
        DATA_TYPE
    FROM
        {DWH_DB}.INFORMATION_SCHEMA.COLUMNS
    WHERE
        TABLE_SCHEMA = '{DWH_SCHEMA}' AND
        TABLE_TYPE = 'BASE TABLE' AND
        DATA_TYPE IN ('TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ', 'DATE', 'TIME');
    """

    logger.info(f"Fetching datetime columns from {DWH_DB}.{DWH_SCHEMA}...")

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

            if not results:
                logger.info("No datetime columns found in the specified DWH database and schema.")
                return {}

            for row in results:
                table_name = row[0]
                column_name = row[1]
                data_type = row[2]
                
                if table_name not in datetime_columns_by_table:
                    datetime_columns_by_table[table_name] = []
                datetime_columns_by_table[table_name].append({
                    "column_name": column_name,
                    "data_type": data_type
                })
        
        logger.info("Finished fetching datetime column information.")
        return datetime_columns_by_table

    except Exception as e:
        logger.error(f"Error fetching datetime columns: {e}")
        return {}


def main():
    """Main function to run the datetime column fetching process."""
    logger.info("--- Starting Snowflake Datetime Column Fetcher ---")
    
    conn = None
    try:
        # --- 1. Connect to Snowflake ---
        conn = create_snowflake_connection()
        
        # --- 2. Fetch Datetime Columns ---
        datetime_cols = fetch_datetime_columns(conn)

        # --- 3. Print Results ---
        if datetime_cols:
            logger.info("\n--- Datetime Columns in DWH Tables ---")
            for table_name, columns in datetime_cols.items():
                print(f"Table: {table_name}")
                for col_info in columns:
                    print(f"  - {col_info['column_name']} (Type: {col_info['data_type']})")
            logger.info("\n--- Fetching completed successfully ---")
        else:
            logger.info("No datetime columns found or an error occurred.")
            
    except Exception as e:
        logger.error(f"An error occurred in the main process: {e}")
        sys.exit(1)
            
    finally:
        # --- 4. Close connection ---
        if conn:
            conn.close()
            logger.info("Snowflake connection closed.")
            
    logger.info("--- Snowflake Datetime Column Fetcher Finished ---")

if __name__ == "__main__":
    main()