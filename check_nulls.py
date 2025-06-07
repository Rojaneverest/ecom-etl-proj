import os
import sys
import snowflake.connector
from dotenv import load_dotenv
import logging

# --- Load Environment Variables ---
load_dotenv()

# --- Configuration ---
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

TARGET_DB = os.getenv("SNOWFLAKE_TARGET_DB", "ECOM_STG") # Default to ECOM_STG if not set
TARGET_SCHEMA = os.getenv("SNOWFLAKE_TARGET_SCHEMA", "PUBLIC") # Default to PUBLIC if not set

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("snowflake_null_checker")

# --- Utility Functions ---

def execute_query(conn, query, fetch_results=False):
    """
    Executes a single SQL query in Snowflake.
    Optionally fetches and returns results.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            logger.debug(f"Query executed successfully: {query}") # Changed to debug
            if fetch_results:
                return cursor.fetchall()
            return None
    except Exception as e:
        logger.error(f"Failed to execute query: {e}\nQuery: {query}")
        raise

def get_stg_table_names():
    """
    Returns a list of staging table names that should be checked.
    This list is derived from the DDL keys, but you can adjust it.
    """
    # Using the same order as in your DDL for consistency
    return [
        "STG_GEOLOCATION", "STG_PRODUCTS", "STG_PRODUCT_CATEGORY_TRANSLATION",
        "STG_CUSTOMERS", "STG_SELLERS", "STG_ORDERS", "STG_ORDER_ITEMS",
        "STG_ORDER_PAYMENTS", "STG_ORDER_REVIEWS"
    ]

def get_table_columns(conn, table_name):
    """
    Fetches column names for a given table from Snowflake's information schema.
    """
    query = f"""
        SELECT COLUMN_NAME
        FROM {TARGET_DB}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{TARGET_SCHEMA}'
          AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION;
    """
    logger.debug(f"Fetching columns for {table_name}")
    columns_data = execute_query(conn, query, fetch_results=True)
    if columns_data:
        return [col[0] for col in columns_data]
    return []

def check_nulls_in_table(conn, table_name, columns):
    """
    Checks for null values in each specified column of a table
    and logs the count if nulls are found.
    """
    logger.info(f"--- Checking nulls in {table_name} ---")
    
    null_found = False
    for col in columns:
        # Count nulls for each column
        count_query = f"""
            SELECT COUNT(*) FROM {TARGET_DB}.{TARGET_SCHEMA}.{table_name}
            WHERE {col} IS NULL;
        """
        null_count_result = execute_query(conn, count_query, fetch_results=True)
        null_count = null_count_result[0][0] if null_count_result else 0

        if null_count > 0:
            null_found = True
            logger.warning(
                f"  Column '{col}': {null_count} NULL values found."
            )
        else:
            logger.info(f"  Column '{col}': No NULL values found.")
            
    if not null_found:
        logger.info(f"No NULL values found in any checked column of {table_name}.")
    logger.info(f"--- Finished checking {table_name} ---")

# --- Main Execution ---

def main():
    """Main function to run the Snowflake null checking process."""
    logger.info("--- Starting Snowflake Null Checker Script ---")

    conn = None
    try:
        # --- 1. Connect to Snowflake ---
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            role=SNOWFLAKE_ROLE,
            database=TARGET_DB,  # Set default database for the session
            schema=TARGET_SCHEMA  # Set default schema for the session
        )
        logger.info("Successfully connected to Snowflake.")

        # Get the list of tables to check
        table_names_to_check = get_stg_table_names()

        # Iterate through each staging table and check for nulls
        for table_name in table_names_to_check:
            columns = get_table_columns(conn, table_name)
            if columns:
                check_nulls_in_table(conn, table_name, columns)
            else:
                logger.warning(f"No columns found for table {table_name} in {TARGET_DB}.{TARGET_SCHEMA}.")

    except Exception as e:
        logger.error(f"An error occurred during the null checking process: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)
    finally:
        # --- 4. Close connection ---
        if conn:
            conn.close()
            logger.info("Snowflake connection closed.")

    logger.info("--- Snowflake Null Checker Script Finished Successfully ---")

if __name__ == "__main__":
    main()