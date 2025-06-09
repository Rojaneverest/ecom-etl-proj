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
STG_DB = os.getenv("SNOWFLAKE_STAGING_DB")
STG_SCHEMA = os.getenv("SNOWFLAKE_STAGING_SCHEMA")


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
            CREATE OR REPLACE TABLE {STG_DB}.{STG_SCHEMA}.STG_GEOLOCATION (
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
            CREATE OR REPLACE TABLE {STG_DB}.{STG_SCHEMA}.STG_PRODUCTS (
                PRODUCT_ID VARCHAR NOT NULL PRIMARY KEY,
                PRODUCT_CATEGORY_NAME VARCHAR,
                PRODUCT_PHOTOS_QTY INTEGER,
                PRODUCT_WEIGHT_G FLOAT,
                PRODUCT_LENGTH_CM FLOAT,
                PRODUCT_HEIGHT_CM FLOAT,
                PRODUCT_WIDTH_CM FLOAT
            );
        """,
        "product_category_translation": f"""
            CREATE OR REPLACE TABLE {STG_DB}.{STG_SCHEMA}.STG_PRODUCT_CATEGORY_TRANSLATION (
                PRODUCT_CATEGORY_NAME VARCHAR NOT NULL PRIMARY KEY,
                PRODUCT_CATEGORY_NAME_ENGLISH VARCHAR NOT NULL
            );
        """,
        # Tables with dependencies
        "customers": f"""
            CREATE OR REPLACE TABLE {STG_DB}.{STG_SCHEMA}.STG_CUSTOMERS (
                CUSTOMER_ID VARCHAR NOT NULL PRIMARY KEY,
                CUSTOMER_UNIQUE_ID VARCHAR,
                CUSTOMER_ZIP_CODE_PREFIX INT,
                CUSTOMER_CITY VARCHAR,
                CUSTOMER_STATE VARCHAR,
                GEOLOCATION_ID INTEGER,
                FOREIGN KEY (GEOLOCATION_ID) REFERENCES {STG_DB}.{STG_SCHEMA}.STG_GEOLOCATION(GEOLOCATION_ID)
            );
        """,
        "sellers": f"""
            CREATE OR REPLACE TABLE {STG_DB}.{STG_SCHEMA}.STG_SELLERS (
                SELLER_ID VARCHAR NOT NULL PRIMARY KEY,
                SELLER_ZIP_CODE_PREFIX INT,
                SELLER_CITY VARCHAR,
                SELLER_STATE VARCHAR,
                GEOLOCATION_ID INTEGER,
                FOREIGN KEY (GEOLOCATION_ID) REFERENCES {STG_DB}.{STG_SCHEMA}.STG_GEOLOCATION(GEOLOCATION_ID)
            );
        """,
        "orders": f"""
            CREATE OR REPLACE TABLE {STG_DB}.{STG_SCHEMA}.STG_ORDERS (
                ORDER_ID VARCHAR NOT NULL PRIMARY KEY,
                CUSTOMER_ID VARCHAR,
                ORDER_STATUS VARCHAR,
                ORDER_PURCHASE_TIMESTAMP TIMESTAMP_NTZ,
                ORDER_APPROVED_AT TIMESTAMP_NTZ,
                ORDER_DELIVERED_CARRIER_DATE TIMESTAMP_NTZ,
                ORDER_DELIVERED_CUSTOMER_DATE TIMESTAMP_NTZ,
                ORDER_ESTIMATED_DELIVERY_DATE TIMESTAMP_NTZ,
                FOREIGN KEY (CUSTOMER_ID) REFERENCES {STG_DB}.{STG_SCHEMA}.STG_CUSTOMERS(CUSTOMER_ID)
            );
        """,
        "order_items": f"""
            CREATE OR REPLACE TABLE {STG_DB}.{STG_SCHEMA}.STG_ORDER_ITEMS (
                ORDER_ID VARCHAR NOT NULL,
                ORDER_ITEM_ID INTEGER NOT NULL,
                PRODUCT_ID VARCHAR,
                SELLER_ID VARCHAR,
                SHIPPING_LIMIT_DATE TIMESTAMP_NTZ,
                PRICE FLOAT,
                FREIGHT_VALUE FLOAT,
                PRIMARY KEY (ORDER_ID, ORDER_ITEM_ID),
                FOREIGN KEY (ORDER_ID) REFERENCES {STG_DB}.{STG_SCHEMA}.STG_ORDERS(ORDER_ID),
                FOREIGN KEY (PRODUCT_ID) REFERENCES {STG_DB}.{STG_SCHEMA}.STG_PRODUCTS(PRODUCT_ID),
                FOREIGN KEY (SELLER_ID) REFERENCES {STG_DB}.{STG_SCHEMA}.STG_SELLERS(SELLER_ID)
            );
        """,
        "order_payments": f"""
            CREATE OR REPLACE TABLE {STG_DB}.{STG_SCHEMA}.STG_ORDER_PAYMENTS (
                ORDER_ID VARCHAR NOT NULL,
                PAYMENT_SEQUENTIAL INTEGER NOT NULL,
                PAYMENT_TYPE VARCHAR,
                PAYMENT_INSTALLMENTS INTEGER,
                PAYMENT_VALUE FLOAT,
                PRIMARY KEY (ORDER_ID, PAYMENT_SEQUENTIAL),
                FOREIGN KEY (ORDER_ID) REFERENCES {STG_DB}.{STG_SCHEMA}.STG_ORDERS(ORDER_ID)
            );
        """,
        "order_reviews": f"""
            CREATE OR REPLACE TABLE {STG_DB}.{STG_SCHEMA}.STG_ORDER_REVIEWS (
                REVIEW_ID VARCHAR NOT NULL PRIMARY KEY,
                ORDER_ID VARCHAR,
                REVIEW_SCORE INTEGER,
                REVIEW_COMMENT_TITLE VARCHAR,
                REVIEW_COMMENT_MESSAGE VARCHAR,
                REVIEW_CREATION_DATE TIMESTAMP_NTZ,
                REVIEW_ANSWER_TIMESTAMP TIMESTAMP_NTZ,
                FOREIGN KEY (ORDER_ID) REFERENCES {STG_DB}.{STG_SCHEMA}.STG_ORDERS(ORDER_ID)
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
    logger.info(f"Ensuring database '{STG_DB}' and schema '{STG_SCHEMA}' exist...")
    execute_query(conn, f"CREATE DATABASE IF NOT EXISTS {STG_DB};")
    execute_query(conn, f"CREATE SCHEMA IF NOT EXISTS {STG_DB}.{STG_SCHEMA};")

def create_stg_tables(conn):
    """Creates the tables in the target staging database."""
    ddl_statements = get_stg_schema_ddl()
    # Define creation order to respect foreign key dependencies
    creation_order = [
        "geolocation", "product_category_translation", "products", # Moved product_category_translation before products
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
        INSERT INTO {STG_DB}.{STG_SCHEMA}.STG_GEOLOCATION (GEOLOCATION_ZIP_CODE_PREFIX, GEOLOCATION_LAT, GEOLOCATION_LNG, GEOLOCATION_CITY, GEOLOCATION_STATE)
        SELECT
            GEOLOCATION_ZIP_CODE_PREFIX,
            GEOLOCATION_LAT,
            GEOLOCATION_LNG,
            LOWER(TRIM(REGEXP_REPLACE(GEOLOCATION_CITY, '[^a-zA-Z0-9 ]', ''))), -- Basic normalization
            GEOLOCATION_STATE
        FROM {SOURCE_DB}.PUBLIC.ODS_GEOLOCATION
        QUALIFY ROW_NUMBER() OVER(PARTITION BY GEOLOCATION_ZIP_CODE_PREFIX, GEOLOCATION_LAT, GEOLOCATION_LNG ORDER BY GEOLOCATION_STATE) = 1;
    """)

    logger.info("Transforming and loading PRODUCT_CATEGORY_TRANSLATION...")
    execute_query(conn, f"""
        INSERT INTO {STG_DB}.{STG_SCHEMA}.STG_PRODUCT_CATEGORY_TRANSLATION
        SELECT PRODUCT_CATEGORY_NAME, PRODUCT_CATEGORY_NAME_ENGLISH
        FROM {SOURCE_DB}.PUBLIC.ODS_PRODUCT_CATEGORY_TRANSLATION
        QUALIFY ROW_NUMBER() OVER(PARTITION BY PRODUCT_CATEGORY_NAME ORDER BY PRODUCT_CATEGORY_NAME_ENGLISH) = 1;
    """)

    # Products Transformation with translation
    logger.info("Transforming and loading PRODUCTS with null handling and category translation...")
    execute_query(conn, f"""
        INSERT INTO {STG_DB}.{STG_SCHEMA}.STG_PRODUCTS
        WITH product_means AS (
            SELECT
                AVG(PRODUCT_WEIGHT_G) AS avg_weight,
                AVG(PRODUCT_LENGTH_CM) AS avg_length,
                AVG(PRODUCT_HEIGHT_CM) AS avg_height,
                AVG(PRODUCT_WIDTH_CM) AS avg_width
            FROM {SOURCE_DB}.PUBLIC.ODS_PRODUCTS
        )
        SELECT
            p.PRODUCT_ID,
            COALESCE(t.PRODUCT_CATEGORY_NAME_ENGLISH, p.PRODUCT_CATEGORY_NAME, 'others') AS PRODUCT_CATEGORY_NAME,
            NVL(p.PRODUCT_PHOTOS_QTY, 0) AS PRODUCT_PHOTOS_QTY,
            NVL(p.PRODUCT_WEIGHT_G, pm.avg_weight) AS PRODUCT_WEIGHT_G,
            NVL(p.PRODUCT_LENGTH_CM, pm.avg_length) AS PRODUCT_LENGTH_CM,
            NVL(p.PRODUCT_HEIGHT_CM, pm.avg_height) AS PRODUCT_HEIGHT_CM,
            NVL(p.PRODUCT_WIDTH_CM, pm.avg_width) AS PRODUCT_WIDTH_CM
        FROM {SOURCE_DB}.PUBLIC.ODS_PRODUCTS p
        LEFT JOIN {STG_DB}.{STG_SCHEMA}.STG_PRODUCT_CATEGORY_TRANSLATION t
            ON p.PRODUCT_CATEGORY_NAME = t.PRODUCT_CATEGORY_NAME
        CROSS JOIN product_means pm;
    """)

    # Create a temporary, deterministic mapping from zip code to geolocation_id
    geolocation_map_sql = f"""
        CREATE OR REPLACE TEMPORARY TABLE geolocation_map AS
        SELECT GEOLOCATION_ZIP_CODE_PREFIX, GEOLOCATION_ID
        FROM {STG_DB}.{STG_SCHEMA}.STG_GEOLOCATION
        QUALIFY ROW_NUMBER() OVER(PARTITION BY GEOLOCATION_ZIP_CODE_PREFIX ORDER BY GEOLOCATION_LAT ASC) = 1;
    """
    logger.info("Creating temporary geolocation map...")
    execute_query(conn, geolocation_map_sql)

    # Add a fallback geolocation record for missing zip codes
    logger.info("Creating fallback geolocation record...")
    execute_query(conn, f"""
        INSERT INTO {STG_DB}.{STG_SCHEMA}.STG_GEOLOCATION (GEOLOCATION_ZIP_CODE_PREFIX, GEOLOCATION_LAT, GEOLOCATION_LNG, GEOLOCATION_CITY, GEOLOCATION_STATE)
        SELECT -1, 0.0, 0.0, 'Unknown Location', 'Unknown'
        WHERE NOT EXISTS (
            SELECT 1 FROM {STG_DB}.{STG_SCHEMA}.STG_GEOLOCATION 
            WHERE GEOLOCATION_ZIP_CODE_PREFIX = -1
        );
    """)

    # Update geolocation map to include fallback
    execute_query(conn, f"""
        INSERT INTO geolocation_map
        SELECT -1, GEOLOCATION_ID
        FROM {STG_DB}.{STG_SCHEMA}.STG_GEOLOCATION
        WHERE GEOLOCATION_ZIP_CODE_PREFIX = -1;
    """)

    # Customers and Sellers (join with geolocation map, handle NULL geolocation_id)
    logger.info("Transforming and loading CUSTOMERS with NULL handling...")
    execute_query(conn, f"""
        INSERT INTO {STG_DB}.{STG_SCHEMA}.STG_CUSTOMERS
        SELECT
            c.CUSTOMER_ID,
            c.CUSTOMER_UNIQUE_ID,
            c.CUSTOMER_ZIP_CODE_PREFIX,
            c.CUSTOMER_CITY,
            c.CUSTOMER_STATE,
            COALESCE(g.GEOLOCATION_ID, fallback.GEOLOCATION_ID) AS GEOLOCATION_ID
        FROM {SOURCE_DB}.PUBLIC.ODS_CUSTOMERS c
        LEFT JOIN geolocation_map g ON c.CUSTOMER_ZIP_CODE_PREFIX = g.GEOLOCATION_ZIP_CODE_PREFIX
        CROSS JOIN (SELECT GEOLOCATION_ID FROM geolocation_map WHERE GEOLOCATION_ZIP_CODE_PREFIX = -1) fallback;
    """)
    
    logger.info("Transforming and loading SELLERS with NULL handling...")
    execute_query(conn, f"""
        INSERT INTO {STG_DB}.{STG_SCHEMA}.STG_SELLERS
        SELECT
            s.SELLER_ID,
            s.SELLER_ZIP_CODE_PREFIX,
            s.SELLER_CITY,
            s.SELLER_STATE,
            COALESCE(g.GEOLOCATION_ID, fallback.GEOLOCATION_ID) AS GEOLOCATION_ID
        FROM {SOURCE_DB}.PUBLIC.ODS_SELLERS s
        LEFT JOIN geolocation_map g ON s.SELLER_ZIP_CODE_PREFIX = g.GEOLOCATION_ZIP_CODE_PREFIX
        CROSS JOIN (SELECT GEOLOCATION_ID FROM geolocation_map WHERE GEOLOCATION_ZIP_CODE_PREFIX = -1) fallback;
    """)

    # Orders (handle NULL timestamp values for ALL timestamp columns)
    logger.info("Transforming and loading ORDERS with NULL timestamp handling...")
    execute_query(conn, f"""
        INSERT INTO {STG_DB}.{STG_SCHEMA}.STG_ORDERS
        SELECT
            ORDER_ID,
            CUSTOMER_ID,
            ORDER_STATUS,
            -- Apply COALESCE to all timestamp columns that are NOT NULL in the DWH
            COALESCE(ORDER_PURCHASE_TIMESTAMP, '1900-01-01 00:00:00'::TIMESTAMP_NTZ) AS ORDER_PURCHASE_TIMESTAMP,
            COALESCE(ORDER_APPROVED_AT, '1900-01-01 00:00:00'::TIMESTAMP_NTZ) AS ORDER_APPROVED_AT,
            COALESCE(ORDER_DELIVERED_CARRIER_DATE, '1900-01-01 00:00:00'::TIMESTAMP_NTZ) AS ORDER_DELIVERED_CARRIER_DATE,
            COALESCE(ORDER_DELIVERED_CUSTOMER_DATE, '1900-01-01 00:00:00'::TIMESTAMP_NTZ) AS ORDER_DELIVERED_CUSTOMER_DATE,
            COALESCE(ORDER_ESTIMATED_DELIVERY_DATE, '1900-01-01 00:00:00'::TIMESTAMP_NTZ) AS ORDER_ESTIMATED_DELIVERY_DATE
        FROM {SOURCE_DB}.PUBLIC.ODS_ORDERS;
    """)

    # Dependent tables
    logger.info("Transforming and loading ORDER_ITEMS with NULL timestamp handling...")
    execute_query(conn, f"""
        INSERT INTO {STG_DB}.{STG_SCHEMA}.STG_ORDER_ITEMS
        SELECT
            ORDER_ID,
            ORDER_ITEM_ID,
            PRODUCT_ID,
            SELLER_ID,
            COALESCE(SHIPPING_LIMIT_DATE, '1900-01-01 00:00:00'::TIMESTAMP_NTZ) AS SHIPPING_LIMIT_DATE,
            PRICE,
            FREIGHT_VALUE
        FROM {SOURCE_DB}.PUBLIC.ODS_ORDER_ITEMS;
    """)

    logger.info("Transforming and loading ORDER_PAYMENTS...")
    execute_query(conn, f"""
        INSERT INTO {STG_DB}.{STG_SCHEMA}.STG_ORDER_PAYMENTS
        SELECT
            ORDER_ID,
            PAYMENT_SEQUENTIAL,
            PAYMENT_TYPE,
            PAYMENT_INSTALLMENTS,
            PAYMENT_VALUE
        FROM {SOURCE_DB}.PUBLIC.ODS_ORDER_PAYMENTS;
    """)

    # Order Reviews: Handle nulls for comments and timestamps
    logger.info("Transforming and loading ORDER_REVIEWS with null handling for comments and timestamps...")
    execute_query(conn, f"""
        INSERT INTO {STG_DB}.{STG_SCHEMA}.STG_ORDER_REVIEWS
        SELECT
            REVIEW_ID,
            ORDER_ID,
            REVIEW_SCORE,
            NVL(REVIEW_COMMENT_TITLE, 'None') AS REVIEW_COMMENT_TITLE,
            NVL(REVIEW_COMMENT_MESSAGE, 'None') AS REVIEW_COMMENT_MESSAGE,
            COALESCE(REVIEW_CREATION_DATE, '1900-01-01 00:00:00'::TIMESTAMP_NTZ) AS REVIEW_CREATION_DATE,
            COALESCE(REVIEW_ANSWER_TIMESTAMP, '1900-01-01 00:00:00'::TIMESTAMP_NTZ) AS REVIEW_ANSWER_TIMESTAMP
        FROM {SOURCE_DB}.PUBLIC.ODS_ORDER_REVIEWS
        QUALIFY ROW_NUMBER() OVER(PARTITION BY REVIEW_ID ORDER BY REVIEW_CREATION_DATE DESC NULLS LAST) = 1;
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

        # Set the current database and schema for the session
        execute_query(conn, f"USE DATABASE {STG_DB};")
        execute_query(conn, f"USE SCHEMA {STG_SCHEMA};")

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