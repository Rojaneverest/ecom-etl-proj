import os
import sys
import snowflake.connector
from dotenv import load_dotenv
import logging


load_dotenv()

# --- Configuration ---
# Snowflake Connection Configuration
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

# Source (Staging) and Target (DWH) Database/Schema
STG_DB = os.getenv("SNOWFLAKE_STAGING_DB", "ECOM_STG")
STG_SCHEMA = os.getenv("SNOWFLAKE_STAGING_SCHEMA", "PUBLIC")
DWH_DB = os.getenv("SNOWFLAKE_DWH_DB", "ECOM_DWH")
DWH_SCHEMA = os.getenv("SNOWFLAKE_DWH_SCHEMA", "PUBLIC")


# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("snowflake_dwh_script")


# --- Schema Definition for Data Warehouse (DWH) ---

def get_dwh_schema_ddl():
    """
    Returns DDL statements for creating the target DWH tables.
    All columns are set to NOT NULL.
    Auditing columns (INSERTION_DATE, MODIFICATION_DATE) are added with default current date.
    """
    return {
        # Tables with no dependencies
        "geolocation": f"""
            CREATE OR REPLACE TABLE {DWH_DB}.{DWH_SCHEMA}.DWH_GEOLOCATION (
                GEOLOCATION_ID NUMBER NOT NULL PRIMARY KEY,
                GEOLOCATION_ZIP_CODE_PREFIX INTEGER NOT NULL,
                GEOLOCATION_LAT FLOAT NOT NULL,
                GEOLOCATION_LNG FLOAT NOT NULL,
                GEOLOCATION_CITY VARCHAR NOT NULL,
                GEOLOCATION_STATE VARCHAR NOT NULL,
                INSERTION_DATE DATE NOT NULL DEFAULT CURRENT_DATE(),
                MODIFICATION_DATE DATE NOT NULL DEFAULT CURRENT_DATE()
            );
        """,
        "products": f"""
            CREATE OR REPLACE TABLE {DWH_DB}.{DWH_SCHEMA}.DWH_PRODUCTS (
                PRODUCT_ID VARCHAR NOT NULL PRIMARY KEY,
                PRODUCT_CATEGORY_NAME VARCHAR NOT NULL,
                PRODUCT_PHOTOS_QTY INTEGER NOT NULL,
                PRODUCT_WEIGHT_G FLOAT NOT NULL,
                PRODUCT_LENGTH_CM FLOAT NOT NULL,
                PRODUCT_HEIGHT_CM FLOAT NOT NULL,
                PRODUCT_WIDTH_CM FLOAT NOT NULL,
                INSERTION_DATE DATE NOT NULL DEFAULT CURRENT_DATE(),
                MODIFICATION_DATE DATE NOT NULL DEFAULT CURRENT_DATE()
            );
        """,
        # Tables with dependencies
        "customers": f"""
            CREATE OR REPLACE TABLE {DWH_DB}.{DWH_SCHEMA}.DWH_CUSTOMERS (
                CUSTOMER_ID VARCHAR NOT NULL PRIMARY KEY,
                CUSTOMER_UNIQUE_ID VARCHAR NOT NULL,
                CUSTOMER_ZIP_CODE_PREFIX INT NOT NULL,
                CUSTOMER_CITY VARCHAR NOT NULL,
                CUSTOMER_STATE VARCHAR NOT NULL,
                GEOLOCATION_ID INTEGER NOT NULL,
                INSERTION_DATE DATE NOT NULL DEFAULT CURRENT_DATE(),
                MODIFICATION_DATE DATE NOT NULL DEFAULT CURRENT_DATE(),
                FOREIGN KEY (GEOLOCATION_ID) REFERENCES {DWH_DB}.{DWH_SCHEMA}.DWH_GEOLOCATION(GEOLOCATION_ID)
            );
        """,
        "sellers": f"""
            CREATE OR REPLACE TABLE {DWH_DB}.{DWH_SCHEMA}.DWH_SELLERS (
                SELLER_ID VARCHAR NOT NULL PRIMARY KEY,
                SELLER_ZIP_CODE_PREFIX INT NOT NULL,
                SELLER_CITY VARCHAR NOT NULL,
                SELLER_STATE VARCHAR NOT NULL,
                GEOLOCATION_ID INTEGER NOT NULL,
                INSERTION_DATE DATE NOT NULL DEFAULT CURRENT_DATE(),
                MODIFICATION_DATE DATE NOT NULL DEFAULT CURRENT_DATE(),
                FOREIGN KEY (GEOLOCATION_ID) REFERENCES {DWH_DB}.{DWH_SCHEMA}.DWH_GEOLOCATION(GEOLOCATION_ID)
            );
        """,
        "orders": f"""
            CREATE OR REPLACE TABLE {DWH_DB}.{DWH_SCHEMA}.DWH_ORDERS (
                ORDER_ID VARCHAR NOT NULL PRIMARY KEY,
                CUSTOMER_ID VARCHAR NOT NULL,
                ORDER_STATUS VARCHAR NOT NULL,
                ORDER_PURCHASE_TIMESTAMP TIMESTAMP_NTZ NOT NULL,
                ORDER_APPROVED_AT TIMESTAMP_NTZ NOT NULL,
                ORDER_DELIVERED_CARRIER_DATE TIMESTAMP_NTZ NOT NULL,
                ORDER_DELIVERED_CUSTOMER_DATE TIMESTAMP_NTZ NOT NULL,
                ORDER_ESTIMATED_DELIVERY_DATE TIMESTAMP_NTZ NOT NULL,
                INSERTION_DATE DATE NOT NULL DEFAULT CURRENT_DATE(),
                MODIFICATION_DATE DATE NOT NULL DEFAULT CURRENT_DATE(),
                FOREIGN KEY (CUSTOMER_ID) REFERENCES {DWH_DB}.{DWH_SCHEMA}.DWH_CUSTOMERS(CUSTOMER_ID)
            );
        """,
        "order_items": f"""
            CREATE OR REPLACE TABLE {DWH_DB}.{DWH_SCHEMA}.DWH_ORDER_ITEMS (
                ORDER_ID VARCHAR NOT NULL,
                ORDER_ITEM_ID INTEGER NOT NULL,
                PRODUCT_ID VARCHAR NOT NULL,
                SELLER_ID VARCHAR NOT NULL,
                SHIPPING_LIMIT_DATE TIMESTAMP_NTZ NOT NULL,
                PRICE FLOAT NOT NULL,
                FREIGHT_VALUE FLOAT NOT NULL,
                INSERTION_DATE DATE NOT NULL DEFAULT CURRENT_DATE(),
                MODIFICATION_DATE DATE NOT NULL DEFAULT CURRENT_DATE(),
                PRIMARY KEY (ORDER_ID, ORDER_ITEM_ID),
                FOREIGN KEY (ORDER_ID) REFERENCES {DWH_DB}.{DWH_SCHEMA}.DWH_ORDERS(ORDER_ID),
                FOREIGN KEY (PRODUCT_ID) REFERENCES {DWH_DB}.{DWH_SCHEMA}.DWH_PRODUCTS(PRODUCT_ID),
                FOREIGN KEY (SELLER_ID) REFERENCES {DWH_DB}.{DWH_SCHEMA}.DWH_SELLERS(SELLER_ID)
            );
        """,
        "order_payments": f"""
            CREATE OR REPLACE TABLE {DWH_DB}.{DWH_SCHEMA}.DWH_ORDER_PAYMENTS (
                ORDER_ID VARCHAR NOT NULL,
                PAYMENT_SEQUENTIAL INTEGER NOT NULL,
                PAYMENT_TYPE VARCHAR NOT NULL,
                PAYMENT_INSTALLMENTS INTEGER NOT NULL,
                PAYMENT_VALUE FLOAT NOT NULL,
                INSERTION_DATE DATE NOT NULL DEFAULT CURRENT_date(),
                MODIFICATION_DATE DATE NOT NULL DEFAULT CURRENT_date(),
                PRIMARY KEY (ORDER_ID, PAYMENT_SEQUENTIAL),
                FOREIGN KEY (ORDER_ID) REFERENCES {DWH_DB}.{DWH_SCHEMA}.DWH_ORDERS(ORDER_ID)
            );
        """,
        "order_reviews": f"""
            CREATE OR REPLACE TABLE {DWH_DB}.{DWH_SCHEMA}.DWH_ORDER_REVIEWS (
                REVIEW_ID VARCHAR NOT NULL PRIMARY KEY,
                ORDER_ID VARCHAR NOT NULL,
                REVIEW_SCORE INTEGER NOT NULL,
                REVIEW_COMMENT_TITLE VARCHAR NOT NULL,
                REVIEW_COMMENT_MESSAGE VARCHAR NOT NULL,
                REVIEW_CREATION_DATE TIMESTAMP_NTZ NOT NULL,
                REVIEW_ANSWER_TIMESTAMP TIMESTAMP_NTZ NOT NULL,
                INSERTION_DATE DATE NOT NULL DEFAULT CURRENT_DATE(),
                MODIFICATION_DATE DATE NOT NULL DEFAULT CURRENT_DATE(),
                FOREIGN KEY (ORDER_ID) REFERENCES {DWH_DB}.{DWH_SCHEMA}.DWH_ORDERS(ORDER_ID)
            );
        """
    }

def execute_query(conn, query):
    """Executes a single SQL query in Snowflake."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
        logger.info("Query executed successfully.")
    except Exception as e:
        logger.error(f"Failed to execute query: {e}\nQuery: {query}")
        raise

def create_dwh_database_and_schema(conn):
    """Creates the target DWH database and schema if they don't exist."""
    logger.info(f"Ensuring database '{DWH_DB}' and schema '{DWH_SCHEMA}' exist...")
    execute_query(conn, f"CREATE DATABASE IF NOT EXISTS {DWH_DB};")
    execute_query(conn, f"CREATE SCHEMA IF NOT EXISTS {DWH_DB}.{DWH_SCHEMA};")

def create_dwh_tables(conn):
    """Creates the tables in the target DWH database."""
    ddl_statements = get_dwh_schema_ddl()
    # Define creation order to respect foreign key dependencies
    creation_order = [
        "geolocation", "products", "customers", "sellers", "orders",
        "order_items", "order_payments", "order_reviews"
    ]
    
    logger.info("Creating DWH tables in the correct order...")
    for table_name in creation_order:
        logger.info(f"Creating table: DWH_{table_name.upper()}...")
        execute_query(conn, ddl_statements[table_name])

def load_data_to_dwh(conn):
    """Loads data from Staging tables to DWH tables."""
    logger.info("--- Starting Data Load from Staging to DWH ---")

    # The order of loading must respect foreign key dependencies.
    loading_order = [
        "geolocation", "products", "customers", "sellers", "orders",
        "order_items", "order_payments", "order_reviews"
    ]
    
    # Get all column names from the staging tables to build the INSERT statements
    stg_tables = {
        "geolocation": "STG_GEOLOCATION", "products": "STG_PRODUCTS",
        "customers": "STG_CUSTOMERS", "sellers": "STG_SELLERS",
        "orders": "STG_ORDERS", "order_items": "STG_ORDER_ITEMS",
        "order_payments": "STG_ORDER_PAYMENTS", "order_reviews": "STG_ORDER_REVIEWS"
    }

    for table_key in loading_order:
        stg_table_name = stg_tables[table_key]
        dwh_table_name = f"DWH_{table_key.upper()}"
        
        logger.info(f"Loading data from {stg_table_name} into {dwh_table_name}...")

        # Dynamically get column list from staging to avoid issues if schema changes
        # Exclude GEOLOCATION_ID from products as it does not exist there
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE TABLE {STG_DB}.{STG_SCHEMA}.{stg_table_name};")
        columns = [row[0] for row in cursor.fetchall()]
        column_list = ", ".join(columns)

        # The INSERTION_DATE and MODIFICATION_DATE columns have defaults and do not need to be in the INSERT statement
        query = f"""
            INSERT INTO {DWH_DB}.{DWH_SCHEMA}.{dwh_table_name} ({column_list})
            SELECT {column_list}
            FROM {STG_DB}.{STG_SCHEMA}.{stg_table_name};
        """
        execute_query(conn, query)

    logger.info("--- Data Loading to DWH Finished ---")


def main():
    """Main function to run the Snowflake DWH loading pipeline."""
    logger.info("--- Starting Snowflake DWH Load Script ---")

    conn = None
    try:
        # --- 1. Connect to Snowflake ---
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            role=SNOWFLAKE_ROLE
        )
        logger.info("Successfully connected to Snowflake.")

        # --- 2. Create Target DWH Database, Schema, and Tables ---
        create_dwh_database_and_schema(conn)

        # Set the current database and schema for the session
        execute_query(conn, f"USE DATABASE {DWH_DB};")
        execute_query(conn, f"USE SCHEMA {DWH_SCHEMA};")

        create_dwh_tables(conn)

        # --- 3. Run the Loading Process from Staging to DWH ---
        load_data_to_dwh(conn)

    except Exception as e:
        logger.error(f"An error occurred during the DWH loading process: {e}")
        sys.exit(1)
    finally:
        # --- 4. Close connection ---
        if conn:
            conn.close()
            logger.info("Snowflake connection closed.")

    logger.info("--- Snowflake DWH Load Script Finished Successfully ---")

if __name__ == "__main__":
    main()