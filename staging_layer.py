import os
import sys
import snowflake.connector
from dotenv import load_dotenv
import logging

# --- Load Environment Variables ---
load_dotenv()

# --- Configuration ---
# Snowflake Database Configuration
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

# Source and Target Database/Schema
SOURCE_DB = os.getenv("SNOWFLAKE_SOURCE_DB")
TARGET_DB = os.getenv("SNOWFLAKE_TARGET_DB")
TARGET_SCHEMA = os.getenv("SNOWFLAKE_TARGET_SCHEMA")


# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("snowflake_elt_script")

# --- Schema and Transformation Logic ---

def get_stg_schema_ddl():
    """
    Returns DDL statements for creating the target ECOM_STG tables.
    The syntax is adapted for Snowflake (e.g., AUTOINCREMENT for SERIAL).
    """
    return {
        # Tables with no dependencies
        "geolocation": f"""
            CREATE OR REPLACE TABLE {TARGET_DB}.{TARGET_SCHEMA}.STG_GEOLOCATION (
                GEOLOCATION_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
                GEOLOCATION_ZIP_CODE_PREFIX INTEGER NOT NULL,
                GEOLOCATION_LAT FLOAT NOT NULL,
                GEOLOCATION_LNG FLOAT NOT NULL,
                GEOLOCATION_CITY VARCHAR,
                GEOLOCATION_STATE VARCHAR,
                UNIQUE (GEOLOCATION_ZIP_CODE_PREFIX, GEOLOCATION_LAT, GEOLOCATION_LNG)
            );
        """,
        "products": f"""
            CREATE OR REPLACE TABLE {TARGET_DB}.{TARGET_SCHEMA}.STG_PRODUCTS (
                PRODUCT_ID VARCHAR NOT NULL PRIMARY KEY,
                PRODUCT_CATEGORY_NAME VARCHAR,
                PRODUCT_NAME_LENGHT INTEGER,
                PRODUCT_DESCRIPTION_LENGHT INTEGER,
                PRODUCT_PHOTOS_QTY INTEGER,
                PRODUCT_WEIGHT_G FLOAT,
                PRODUCT_LENGTH_CM FLOAT,
                PRODUCT_HEIGHT_CM FLOAT,
                PRODUCT_WIDTH_CM FLOAT
            );
        """,
        "product_category_translation": f"""
            CREATE OR REPLACE TABLE {TARGET_DB}.{TARGET_SCHEMA}.STG_PRODUCT_CATEGORY_TRANSLATION (
                PRODUCT_CATEGORY_NAME VARCHAR NOT NULL PRIMARY KEY,
                PRODUCT_CATEGORY_NAME_ENGLISH VARCHAR NOT NULL
            );
        """,
        # Tables with dependencies
        "customers": f"""
            CREATE OR REPLACE TABLE {TARGET_DB}.{TARGET_SCHEMA}.STG_CUSTOMERS (
                CUSTOMER_ID VARCHAR NOT NULL PRIMARY KEY,
                CUSTOMER_UNIQUE_ID VARCHAR,
                CUSTOMER_ZIP_CODE_PREFIX INT,
                CUSTOMER_CITY VARCHAR,
                CUSTOMER_STATE VARCHAR,
                GEOLOCATION_ID INTEGER,
                FOREIGN KEY (GEOLOCATION_ID) REFERENCES {TARGET_DB}.{TARGET_SCHEMA}.STG_GEOLOCATION(GEOLOCATION_ID)
            );
        """,
        "sellers": f"""
            CREATE OR REPLACE TABLE {TARGET_DB}.{TARGET_SCHEMA}.STG_SELLERS (
                SELLER_ID VARCHAR NOT NULL PRIMARY KEY,
                SELLER_ZIP_CODE_PREFIX INT,
                SELLER_CITY VARCHAR,
                SELLER_STATE VARCHAR,
                GEOLOCATION_ID INTEGER,
                FOREIGN KEY (GEOLOCATION_ID) REFERENCES {TARGET_DB}.{TARGET_SCHEMA}.STG_GEOLOCATION(GEOLOCATION_ID)
            );
        """,
        "orders": f"""
            CREATE OR REPLACE TABLE {TARGET_DB}.{TARGET_SCHEMA}.STG_ORDERS (
                ORDER_ID VARCHAR NOT NULL PRIMARY KEY,
                CUSTOMER_ID VARCHAR,
                ORDER_STATUS VARCHAR,
                ORDER_PURCHASE_TIMESTAMP TIMESTAMP_NTZ,
                ORDER_APPROVED_AT TIMESTAMP_NTZ,
                ORDER_DELIVERED_CARRIER_DATE TIMESTAMP_NTZ,
                ORDER_DELIVERED_CUSTOMER_DATE TIMESTAMP_NTZ,
                ORDER_ESTIMATED_DELIVERY_DATE TIMESTAMP_NTZ,
                FOREIGN KEY (CUSTOMER_ID) REFERENCES {TARGET_DB}.{TARGET_SCHEMA}.STG_CUSTOMERS(CUSTOMER_ID)
            );
        """,
        "order_items": f"""
            CREATE OR REPLACE TABLE {TARGET_DB}.{TARGET_SCHEMA}.STG_ORDER_ITEMS (
                ORDER_ID VARCHAR NOT NULL,
                ORDER_ITEM_ID INTEGER NOT NULL,
                PRODUCT_ID VARCHAR,
                SELLER_ID VARCHAR,
                SHIPPING_LIMIT_DATE TIMESTAMP_NTZ,
                PRICE FLOAT,
                FREIGHT_VALUE FLOAT,
                PRIMARY KEY (ORDER_ID, ORDER_ITEM_ID),
                FOREIGN KEY (ORDER_ID) REFERENCES {TARGET_DB}.{TARGET_SCHEMA}.STG_ORDERS(ORDER_ID),
                FOREIGN KEY (PRODUCT_ID) REFERENCES {TARGET_DB}.{TARGET_SCHEMA}.STG_PRODUCTS(PRODUCT_ID),
                FOREIGN KEY (SELLER_ID) REFERENCES {TARGET_DB}.{TARGET_SCHEMA}.STG_SELLERS(SELLER_ID)
            );
        """,
        "order_payments": f"""
            CREATE OR REPLACE TABLE {TARGET_DB}.{TARGET_SCHEMA}.STG_ORDER_PAYMENTS (
                ORDER_ID VARCHAR NOT NULL,
                PAYMENT_SEQUENTIAL INTEGER NOT NULL,
                PAYMENT_TYPE VARCHAR,
                PAYMENT_INSTALLMENTS INTEGER,
                PAYMENT_VALUE FLOAT,
                PRIMARY KEY (ORDER_ID, PAYMENT_SEQUENTIAL),
                FOREIGN KEY (ORDER_ID) REFERENCES {TARGET_DB}.{TARGET_SCHEMA}.STG_ORDERS(ORDER_ID)
            );
        """,
        "order_reviews": f"""
            CREATE OR REPLACE TABLE {TARGET_DB}.{TARGET_SCHEMA}.STG_ORDER_REVIEWS (
                REVIEW_ID VARCHAR NOT NULL PRIMARY KEY,
                ORDER_ID VARCHAR,
                REVIEW_SCORE INTEGER,
                REVIEW_COMMENT_TITLE VARCHAR,
                REVIEW_COMMENT_MESSAGE VARCHAR,
                REVIEW_CREATION_DATE TIMESTAMP_NTZ,
                REVIEW_ANSWER_TIMESTAMP TIMESTAMP_NTZ,
                FOREIGN KEY (ORDER_ID) REFERENCES {TARGET_DB}.{TARGET_SCHEMA}.STG_ORDERS(ORDER_ID)
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

def create_stg_database_and_schema(conn):
    """Creates the target database and schema if they don't exist."""
    logger.info(f"Ensuring database '{TARGET_DB}' and schema '{TARGET_SCHEMA}' exist...")
    execute_query(conn, f"CREATE DATABASE IF NOT EXISTS {TARGET_DB};")
    execute_query(conn, f"CREATE SCHEMA IF NOT EXISTS {TARGET_DB}.{TARGET_SCHEMA};")

def create_stg_tables(conn):
    """Creates the tables in the target staging database."""
    ddl_statements = get_stg_schema_ddl()
    # Define creation order to respect foreign key dependencies
    creation_order = [
        "geolocation", "products", "product_category_translation",
        "customers", "sellers", "orders", "order_items",
        "order_payments", "order_reviews"
    ]
    
    logger.info("Creating staging tables in the correct order...")
    for table_name in creation_order:
        logger.info(f"Creating table: {table_name.upper()}...")
        execute_query(conn, ddl_statements[table_name])

def transform_and_load_data(conn):
    """Executes the SQL-based transformation and load for each table."""
    logger.info("--- Starting Data Transformation and Loading ---")

    # Geolocation: Clean city names and deduplicate
    logger.info("Transforming and loading GEOLOCATION...")
    execute_query(conn, f"""
        INSERT INTO {TARGET_DB}.{TARGET_SCHEMA}.STG_GEOLOCATION (GEOLOCATION_ZIP_CODE_PREFIX, GEOLOCATION_LAT, GEOLOCATION_LNG, GEOLOCATION_CITY, GEOLOCATION_STATE)
        SELECT
            GEOLOCATION_ZIP_CODE_PREFIX,
            GEOLOCATION_LAT,
            GEOLOCATION_LNG,
            LOWER(TRIM(REGEXP_REPLACE(GEOLOCATION_CITY, '[^a-zA-Z0-9 ]', ''))), -- Basic normalization
            GEOLOCATION_STATE
        FROM {SOURCE_DB}.PUBLIC.ODS_GEOLOCATION
        QUALIFY ROW_NUMBER() OVER(PARTITION BY GEOLOCATION_ZIP_CODE_PREFIX, GEOLOCATION_LAT, GEOLOCATION_LNG ORDER BY GEOLOCATION_STATE) = 1;
    """)

    # Products and Translation (direct load with type casting)
    logger.info("Transforming and loading PRODUCTS...")
    execute_query(conn, f"""
        INSERT INTO {TARGET_DB}.{TARGET_SCHEMA}.STG_PRODUCTS
        SELECT
            PRODUCT_ID,
            PRODUCT_CATEGORY_NAME,
            PRODUCT_NAME_LENGHT,
            PRODUCT_DESCRIPTION_LENGHT,
            PRODUCT_PHOTOS_QTY,
            PRODUCT_WEIGHT_G,
            PRODUCT_LENGTH_CM,
            PRODUCT_HEIGHT_CM,
            PRODUCT_WIDTH_CM -- REMOVED: No cast needed as it's already FLOAT
        FROM {SOURCE_DB}.PUBLIC.ODS_PRODUCTS;
    """)

    logger.info("Transforming and loading PRODUCT_CATEGORY_TRANSLATION...")
    execute_query(conn, f"""
        INSERT INTO {TARGET_DB}.{TARGET_SCHEMA}.STG_PRODUCT_CATEGORY_TRANSLATION
        SELECT PRODUCT_CATEGORY_NAME, PRODUCT_CATEGORY_NAME_ENGLISH
        FROM {SOURCE_DB}.PUBLIC.ODS_PRODUCT_CATEGORY_TRANSLATION
        QUALIFY ROW_NUMBER() OVER(PARTITION BY PRODUCT_CATEGORY_NAME ORDER BY PRODUCT_CATEGORY_NAME_ENGLISH) = 1;
    """)

    # Create a temporary, deterministic mapping from zip code to geolocation_id
    geolocation_map_sql = f"""
        CREATE OR REPLACE TEMPORARY TABLE geolocation_map AS
        SELECT GEOLOCATION_ZIP_CODE_PREFIX, GEOLOCATION_ID
        FROM {TARGET_DB}.{TARGET_SCHEMA}.STG_GEOLOCATION
        QUALIFY ROW_NUMBER() OVER(PARTITION BY GEOLOCATION_ZIP_CODE_PREFIX ORDER BY GEOLOCATION_LAT ASC) = 1;
    """
    logger.info("Creating temporary geolocation map...")
    execute_query(conn, geolocation_map_sql)

    # Customers and Sellers (join with geolocation map)
    logger.info("Transforming and loading CUSTOMERS...")
    execute_query(conn, f"""
        INSERT INTO {TARGET_DB}.{TARGET_SCHEMA}.STG_CUSTOMERS
        SELECT
            c.CUSTOMER_ID,
            c.CUSTOMER_UNIQUE_ID,
            c.CUSTOMER_ZIP_CODE_PREFIX,
            c.CUSTOMER_CITY,
            c.CUSTOMER_STATE,
            g.GEOLOCATION_ID
        FROM {SOURCE_DB}.PUBLIC.ODS_CUSTOMERS c
        LEFT JOIN geolocation_map g ON c.CUSTOMER_ZIP_CODE_PREFIX = g.GEOLOCATION_ZIP_CODE_PREFIX;
    """)
    
    logger.info("Transforming and loading SELLERS...")
    execute_query(conn, f"""
        INSERT INTO {TARGET_DB}.{TARGET_SCHEMA}.STG_SELLERS
        SELECT
            s.SELLER_ID,
            s.SELLER_ZIP_CODE_PREFIX,
            s.SELLER_CITY,
            s.SELLER_STATE,
            g.GEOLOCATION_ID
        FROM {SOURCE_DB}.PUBLIC.ODS_SELLERS s
        LEFT JOIN geolocation_map g ON s.SELLER_ZIP_CODE_PREFIX = g.GEOLOCATION_ZIP_CODE_PREFIX;
    """)

    # Orders (direct load with type casting)
    logger.info("Transforming and loading ORDERS...")
    execute_query(conn, f"""
        INSERT INTO {TARGET_DB}.{TARGET_SCHEMA}.STG_ORDERS
        SELECT
            ORDER_ID,
            CUSTOMER_ID,
            ORDER_STATUS,
            ORDER_PURCHASE_TIMESTAMP,
            ORDER_APPROVED_AT,
            ORDER_DELIVERED_CARRIER_DATE,
            ORDER_DELIVERED_CUSTOMER_DATE,
            ORDER_ESTIMATED_DELIVERY_DATE
        FROM {SOURCE_DB}.PUBLIC.ODS_ORDERS;
    """)
    
    # Dependent tables
    logger.info("Transforming and loading ORDER_ITEMS...")
    execute_query(conn, f"""
        INSERT INTO {TARGET_DB}.{TARGET_SCHEMA}.STG_ORDER_ITEMS
        SELECT
            ORDER_ID,
            ORDER_ITEM_ID,
            PRODUCT_ID,
            SELLER_ID,
            SHIPPING_LIMIT_DATE,
            PRICE,
            FREIGHT_VALUE
        FROM {SOURCE_DB}.PUBLIC.ODS_ORDER_ITEMS;
    """)

    logger.info("Transforming and loading ORDER_PAYMENTS...")
    execute_query(conn, f"""
        INSERT INTO {TARGET_DB}.{TARGET_SCHEMA}.STG_ORDER_PAYMENTS
        SELECT
            ORDER_ID,
            PAYMENT_SEQUENTIAL,
            PAYMENT_TYPE,
            PAYMENT_INSTALLMENTS,
            PAYMENT_VALUE
        FROM {SOURCE_DB}.PUBLIC.ODS_ORDER_PAYMENTS;
    """)

    # Order Reviews: Clean null-like text and deduplicate
    logger.info("Transforming and loading ORDER_REVIEWS...")
    execute_query(conn, f"""
        INSERT INTO {TARGET_DB}.{TARGET_SCHEMA}.STG_ORDER_REVIEWS
        WITH cleaned_source AS (
            SELECT
                *,
                CASE WHEN TRIM(LOWER(REVIEW_COMMENT_TITLE)) IN ('nan', 'none', 'null', '', '[null]', '[none]', 'na', '<na>') THEN NULL ELSE REVIEW_COMMENT_TITLE END as CLEANED_TITLE,
                CASE WHEN TRIM(LOWER(REVIEW_COMMENT_MESSAGE)) IN ('nan', 'none', 'null', '', '[null]', '[none]', 'na', '<na>') THEN NULL ELSE REVIEW_COMMENT_MESSAGE END as CLEANED_MESSAGE
            FROM {SOURCE_DB}.PUBLIC.ODS_ORDER_REVIEWS
        )
        SELECT
            REVIEW_ID,
            ORDER_ID,
            REVIEW_SCORE,
            CLEANED_TITLE,
            CLEANED_MESSAGE,
            REVIEW_CREATION_DATE,
            REVIEW_ANSWER_TIMESTAMP
        FROM cleaned_source
        QUALIFY ROW_NUMBER() OVER(PARTITION BY REVIEW_ID ORDER BY REVIEW_CREATION_DATE DESC) = 1;
    """)

    logger.info("--- Data Transformation and Loading Finished ---")


def main():
    """Main function to run the Snowflake ELT pipeline."""
    logger.info("--- Starting Snowflake ELT Script ---")

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

        # --- 2. Create Target Database, Schema, and Tables ---
        create_stg_database_and_schema(conn)

        # --- ADD THESE LINES HERE ---
        # Set the current database and schema for the session
        execute_query(conn, f"USE DATABASE {TARGET_DB};")
        execute_query(conn, f"USE SCHEMA {TARGET_SCHEMA};")
        # --- END ADDITION ---

        create_stg_tables(conn)

        # --- 3. Run Transformation and Load Process ---
        transform_and_load_data(conn)

    except Exception as e:
        logger.error(f"An error occurred during the ELT process: {e}")
        sys.exit(1)
    finally:
        # --- 4. Close connection ---
        if conn:
            conn.close()
            logger.info("Snowflake connection closed.")

    logger.info("--- Snowflake ELT Script Finished Successfully ---")

if __name__ == "__main__":
    main()