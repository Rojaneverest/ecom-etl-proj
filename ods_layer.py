import os
import sys
import boto3
import snowflake.connector
from dotenv import load_dotenv
from botocore.exceptions import ClientError
import logging

# --- Load Environment Variables ---
load_dotenv()

# --- Configuration ---
# AWS Configuration
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")
RAW_DATA_PREFIX = "raw/"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Snowflake Database Configuration
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = "ECOM_ODS"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

# External Stage Name
STAGE_NAME = "S3_RAW_DATA_STAGE"

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ods_snowflake_copy_script")

# --- Mapping S3 Object Names to ODS Table Names ---
S3_TO_ODS_TABLE_MAP = {
    "olist_customers_dataset": "ODS_CUSTOMERS",
    "olist_sellers_dataset": "ODS_SELLERS",
    "olist_products_dataset": "ODS_PRODUCTS",
    "olist_orders_dataset": "ODS_ORDERS",
    "olist_order_items_dataset": "ODS_ORDER_ITEMS",
    "olist_order_payments_dataset": "ODS_ORDER_PAYMENTS",
    "olist_order_reviews_dataset": "ODS_ORDER_REVIEWS",
    "olist_geolocation_dataset": "ODS_GEOLOCATION",
    "product_category_name_translation": "ODS_PRODUCT_CATEGORY_TRANSLATION",
}

# --- Table Creation SQL Statements ---
TABLE_CREATION_SQL = {
    "ODS_GEOLOCATION": """
CREATE TABLE IF NOT EXISTS ODS_GEOLOCATION (
    geolocation_zip_code_prefix INTEGER,
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city VARCHAR,
    geolocation_state VARCHAR,
    UNIQUE (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng)
)
    """,
    
    "ODS_CUSTOMERS": """
CREATE TABLE IF NOT EXISTS ODS_CUSTOMERS (
    customer_id VARCHAR,
    customer_unique_id VARCHAR,
    customer_zip_code_prefix INT,
    customer_city VARCHAR,
    customer_state VARCHAR,
    PRIMARY KEY (customer_id)
)
    """,
    
    "ODS_SELLERS": """
CREATE TABLE IF NOT EXISTS ODS_SELLERS (
    seller_id VARCHAR,
    seller_zip_code_prefix INT,
    seller_city VARCHAR,
    seller_state VARCHAR,
    PRIMARY KEY (seller_id)
)
    """,
    
    "ODS_ORDERS": """
CREATE TABLE IF NOT EXISTS ODS_ORDERS (
    order_id VARCHAR,
    customer_id VARCHAR,
    order_status VARCHAR,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    PRIMARY KEY (order_id)
)
    """,
    
    "ODS_PRODUCTS": """
CREATE TABLE IF NOT EXISTS ODS_PRODUCTS (
    product_id VARCHAR,
    product_category_name VARCHAR,
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g FLOAT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT,
    PRIMARY KEY (product_id)
)
    """,
    
    "ODS_ORDER_ITEMS": """
CREATE TABLE IF NOT EXISTS ODS_ORDER_ITEMS (
    order_id VARCHAR,
    order_item_id INTEGER,
    product_id VARCHAR,
    seller_id VARCHAR,
    shipping_limit_date TIMESTAMP,
    price FLOAT,
    freight_value FLOAT,
    PRIMARY KEY (order_id, order_item_id)
)
    """,
    
    "ODS_ORDER_PAYMENTS": """
CREATE TABLE IF NOT EXISTS ODS_ORDER_PAYMENTS (
    order_id VARCHAR,
    payment_sequential INTEGER,
    payment_type VARCHAR,
    payment_installments INTEGER,
    payment_value FLOAT,
    PRIMARY KEY (order_id, payment_sequential)
)
    """,
    
    "ODS_ORDER_REVIEWS": """
CREATE TABLE IF NOT EXISTS ODS_ORDER_REVIEWS (
    review_id VARCHAR,
    order_id VARCHAR,
    review_score INTEGER,
    review_comment_title VARCHAR,
    review_comment_message VARCHAR,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    PRIMARY KEY (review_id)
)
    """,
    
    "ODS_PRODUCT_CATEGORY_TRANSLATION": """
CREATE TABLE IF NOT EXISTS ODS_PRODUCT_CATEGORY_TRANSLATION (
    product_category_name VARCHAR,
    product_category_name_english VARCHAR,
    PRIMARY KEY (product_category_name)
)
    """
}

# --- Core Functions ---

def create_s3_client():
    """Creates and returns a boto3 S3 client."""
    try:
        s3_client = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        return s3_client
    except Exception as e:
        logger.error(f"Failed to create S3 client: {e}")
        sys.exit(1)

def create_snowflake_connection():
    """Establishes a connection to Snowflake and returns the connection object."""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            role=SNOWFLAKE_ROLE
        )
        logger.info("Successfully connected to Snowflake.")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        sys.exit(1)

def setup_database_and_schema(conn):
    """Creates the ECOM_ODS database and PUBLIC schema if they don't exist."""
    try:
        with conn.cursor() as cursor:
            # Create database
            logger.info("Creating ECOM_ODS database if it doesn't exist...")
            cursor.execute("CREATE DATABASE IF NOT EXISTS ECOM_ODS")
            
            # Use the database
            cursor.execute("USE DATABASE ECOM_ODS")
            
            # Create schema
            logger.info("Creating PUBLIC schema if it doesn't exist...")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS PUBLIC")
            
            # Use the schema
            cursor.execute("USE SCHEMA PUBLIC")
            
            logger.info("Database and schema setup completed successfully.")
            
    except Exception as e:
        logger.error(f"Error setting up database and schema: {e}")
        raise

def create_external_stage(conn):
    """Creates an external stage pointing to the S3 bucket with proper PARQUET file format."""
    try:
        with conn.cursor() as cursor:
            # Ensure we're using the correct database and schema
            cursor.execute("USE DATABASE ECOM_ODS")
            cursor.execute("USE SCHEMA PUBLIC")

            # --- Create a named file format for PARQUET (with valid options only) ---
            file_format_name = "PARQUET_FORMAT_WITH_TIMESTAMP_HANDLING"
            file_format_sql = f"""
            CREATE OR REPLACE FILE FORMAT {file_format_name}
                TYPE = 'PARQUET'
                COMPRESSION = 'AUTO'
                NULL_IF = ('', 'NULL', 'null', 'NaN', 'nan')
                TRIM_SPACE = TRUE
                REPLACE_INVALID_CHARACTERS = TRUE;
            """
            cursor.execute(file_format_sql)
            logger.info(f"Successfully created or replaced file format '{file_format_name}'")

            logger.info(f"Creating external stage '{STAGE_NAME}'...")

            # Drop existing stage if it exists
            cursor.execute(f"DROP STAGE IF EXISTS {STAGE_NAME}")

            # --- Create the external stage using the PARQUET file format ---
            stage_sql = f"""
            CREATE STAGE {STAGE_NAME}
            URL = 's3://{S3_BUCKET}/'
            CREDENTIALS = (
                AWS_KEY_ID = '{AWS_ACCESS_KEY_ID}'
                AWS_SECRET_KEY = '{AWS_SECRET_ACCESS_KEY}'
            )
            FILE_FORMAT = (
                FORMAT_NAME = '{file_format_name}'
            )
            """

            cursor.execute(stage_sql)
            logger.info(f"Successfully created external stage '{STAGE_NAME}'")

    except Exception as e:
        logger.error(f"Error creating external stage: {e}")
        raise

def create_ods_tables(conn):
    """Creates all ODS tables in the ECOM_ODS.PUBLIC schema."""
    try:
        with conn.cursor() as cursor:
            # Ensure we're using the correct database and schema
            cursor.execute("USE DATABASE ECOM_ODS")
            cursor.execute("USE SCHEMA PUBLIC")
            
            logger.info("Creating ODS tables...")
            
            for table_name, create_sql in TABLE_CREATION_SQL.items():
                try:
                    logger.info(f"Creating table {table_name}...")
                    cursor.execute(create_sql)
                    logger.info(f"Successfully created table {table_name}")
                except Exception as e:
                    logger.error(f"Error creating table {table_name}: {e}")
                    raise
                    
            logger.info("All ODS tables created successfully.")
            
    except Exception as e:
        logger.error(f"Error creating ODS tables: {e}")
        raise

def get_latest_parquet_objects(s3_client, bucket, prefix):
    """Lists the latest Parquet files for each dataset in the S3 raw prefix."""
    latest_objects = {}
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        dataset_folders = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/")
        for page in dataset_folders:
            for common_prefix in page.get("CommonPrefixes", []):
                dataset_prefix = common_prefix.get("Prefix")
                files = s3_client.list_objects_v2(Bucket=bucket, Prefix=dataset_prefix)
                if "Contents" in files:
                    parquet_files = [obj for obj in files['Contents'] if obj['Key'].endswith('.parquet')]
                    if parquet_files:
                        latest_file = max(parquet_files, key=lambda obj: obj['LastModified'])
                        dataset_name = dataset_prefix.split('/')[-2]
                        latest_objects[dataset_name] = latest_file['Key']
    except ClientError as e:
        logger.error(f"Error listing S3 objects: {e}")
        return {}
    return latest_objects

def get_table_specific_copy_sql(table_name, s3_key):
    """Returns table-specific COPY INTO SQL with enhanced timestamp handling for problematic tables."""
    
    # Tables with timestamp issues and their specific handling
    timestamp_problematic_tables = {
        'ODS_ORDER_REVIEWS': {
            'columns': ['review_creation_date', 'review_answer_timestamp'],
            'transformations': """
            COPY INTO "ODS_ORDER_REVIEWS" 
            FROM (
                SELECT 
                    $1:review_id::VARCHAR as review_id,
                    $1:order_id::VARCHAR as order_id,
                    $1:review_score::INTEGER as review_score,
                    $1:review_comment_title::VARCHAR as review_comment_title,
                    $1:review_comment_message::VARCHAR as review_comment_message,
                    TRY_TO_TIMESTAMP($1:review_creation_date::VARCHAR, 'YYYY-MM-DD"T"HH24:MI:SS') as review_creation_date,
                    TRY_TO_TIMESTAMP($1:review_answer_timestamp::VARCHAR, 'YYYY-MM-DD"T"HH24:MI:SS') as review_answer_timestamp
                FROM '@{stage_name}/{s3_key}'
            )
            """
        },
        'ODS_ORDER_ITEMS': {
            'columns': ['shipping_limit_date'],
            'transformations': """
            COPY INTO "ODS_ORDER_ITEMS" 
            FROM (
                SELECT 
                    $1:order_id::VARCHAR as order_id,
                    $1:order_item_id::INTEGER as order_item_id,
                    $1:product_id::VARCHAR as product_id,
                    $1:seller_id::VARCHAR as seller_id,
                    TRY_TO_TIMESTAMP($1:shipping_limit_date::VARCHAR, 'YYYY-MM-DD"T"HH24:MI:SS') as shipping_limit_date,
                    $1:price::FLOAT as price,
                    $1:freight_value::FLOAT as freight_value
                FROM '@{stage_name}/{s3_key}'
            )
            """
        },
        'ODS_ORDERS': {
            'columns': ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 
                       'order_delivered_customer_date', 'order_estimated_delivery_date'],
            'transformations': """
            COPY INTO "ODS_ORDERS" 
            FROM (
                SELECT 
                    $1:order_id::VARCHAR as order_id,
                    $1:customer_id::VARCHAR as customer_id,
                    $1:order_status::VARCHAR as order_status,
                    TRY_TO_TIMESTAMP($1:order_purchase_timestamp::VARCHAR, 'YYYY-MM-DD"T"HH24:MI:SS') as order_purchase_timestamp,
                    TRY_TO_TIMESTAMP($1:order_approved_at::VARCHAR, 'YYYY-MM-DD"T"HH24:MI:SS') as order_approved_at,
                    TRY_TO_TIMESTAMP($1:order_delivered_carrier_date::VARCHAR, 'YYYY-MM-DD"T"HH24:MI:SS') as order_delivered_carrier_date,
                    TRY_TO_TIMESTAMP($1:order_delivered_customer_date::VARCHAR, 'YYYY-MM-DD"T"HH24:MI:SS') as order_delivered_customer_date,
                    TRY_TO_TIMESTAMP($1:order_estimated_delivery_date::VARCHAR, 'YYYY-MM-DD"T"HH24:MI:SS') as order_estimated_delivery_date
                FROM '@{stage_name}/{s3_key}'
            )
            """
        }
    }
    
    # Check if this table needs special handling
    if table_name in timestamp_problematic_tables:
        transformation_sql = timestamp_problematic_tables[table_name]['transformations']
        return transformation_sql.format(stage_name=STAGE_NAME, s3_key=s3_key)
    
    # Default COPY INTO for tables without timestamp issues
    return f"""
    COPY INTO "{table_name}"
    FROM '@{STAGE_NAME}/{s3_key}'
    FILE_FORMAT = (
        TYPE = 'PARQUET'
        COMPRESSION = 'AUTO'
    )
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'CONTINUE'
    PURGE = FALSE
    """

def copy_data_from_s3_to_snowflake(conn, s3_key, table_name):
    """
    Uses Snowflake's COPY INTO command with enhanced timestamp handling to load data directly from S3.
    """
    ods_table_name = table_name.upper()

    logger.info(f"Starting COPY INTO for table '{ods_table_name}' from s3://{S3_BUCKET}/{s3_key}...")

    try:
        with conn.cursor() as cursor:
            # Explicitly set context for this operation
            cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

            # Truncate the table to ensure idempotency
            logger.info(f"Truncating table '{ods_table_name}' before loading.")
            cursor.execute(f'TRUNCATE TABLE "{ods_table_name}"')

            # Get table-specific COPY SQL with timestamp handling
            copy_sql = get_table_specific_copy_sql(ods_table_name, s3_key)

            logger.info(f"Executing COPY INTO command for '{ods_table_name}' with timestamp handling...")
            cursor.execute(copy_sql)

            # Get the result to see how many rows were loaded
            rows_loaded = 0
            files_loaded = 0
            errors_seen = 0

            for row in cursor:
                if row and len(row) >= 3:
                    status = row[1] if len(row) > 1 else None
                    if status == 'LOADED':
                        rows_loaded += int(row[3]) if len(row) > 3 and row[3] is not None else 0
                        files_loaded += 1
                    elif status == 'PARTIALLY_LOADED' or status == 'LOAD_FAILED':
                        errors_seen += int(row[5]) if len(row) > 5 and row[5] is not None else 0
                        logger.warning(f"File {row[0]} status: {status}, errors: {row[5] if len(row) > 5 else 'N/A'}")

            if files_loaded > 0:
                logger.info(f"Successfully loaded {rows_loaded} rows into '{ods_table_name}' from {files_loaded} file(s).")
            else:
                logger.warning(f"No files were successfully loaded into '{ods_table_name}'. Total errors: {errors_seen}")

    except Exception as e:
        logger.error(f"Error during COPY INTO for '{ods_table_name}': {e}")
        raise

def validate_data_load(conn, table_name):
    """Validates the data load by checking row count and timestamp data quality."""
    try:
        with conn.cursor() as cursor:
            # Explicitly set context for this operation
            cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

            cursor.execute(f'SELECT COUNT(*) FROM "{table_name.upper()}"')
            row_count = cursor.fetchone()[0]
            logger.info(f"Table '{table_name}' contains {row_count} rows after load.")
            
            # Additional validation for timestamp columns in problematic tables
            timestamp_validation_queries = {
                'ODS_ORDER_REVIEWS': [
                    'SELECT COUNT(*) FROM "ODS_ORDER_REVIEWS" WHERE review_creation_date IS NULL',
                    'SELECT COUNT(*) FROM "ODS_ORDER_REVIEWS" WHERE review_answer_timestamp IS NULL'
                ],
                'ODS_ORDER_ITEMS': [
                    'SELECT COUNT(*) FROM "ODS_ORDER_ITEMS" WHERE shipping_limit_date IS NULL'
                ],
                'ODS_ORDERS': [
                    'SELECT COUNT(*) FROM "ODS_ORDERS" WHERE order_purchase_timestamp IS NULL',
                    'SELECT COUNT(*) FROM "ODS_ORDERS" WHERE order_approved_at IS NULL'
                ]
            }
            
            if table_name.upper() in timestamp_validation_queries:
                logger.info(f"Performing timestamp validation for '{table_name}'...")
                for query in timestamp_validation_queries[table_name.upper()]:
                    cursor.execute(query)
                    null_count = cursor.fetchone()[0]
                    column_name = query.split('WHERE ')[1].split(' IS NULL')[0]
                    logger.info(f"  - {column_name}: {null_count} NULL values out of {row_count} total rows")
            
            return row_count

    except Exception as e:
        logger.error(f"Error validating data load for '{table_name}': {e}")
        return 0

def main():
    """Main function to run the S3 to Snowflake ODS data ingestion pipeline with enhanced timestamp handling."""
    logger.info("--- Starting Enhanced ODS Ingestion Script for Snowflake (with Timestamp Handling) ---")
    
    conn = None
    try:
        # --- 1. Setup connections ---
        s3_client = create_s3_client()
        conn = create_snowflake_connection()
        
        # --- 2. Setup database, schema, stage, and tables ---
        setup_database_and_schema(conn)
        create_external_stage(conn)
        create_ods_tables(conn)
        
        # --- 3. Find latest data in S3 ---
        logger.info(f"Listing latest Parquet files from s3://{S3_BUCKET}/{RAW_DATA_PREFIX}...")
        latest_parquet_objects = get_latest_parquet_objects(s3_client, S3_BUCKET, RAW_DATA_PREFIX)

        if not latest_parquet_objects:
            logger.warning("No Parquet objects found in S3. Exiting.")
            return

        logger.info(f"Found {len(latest_parquet_objects)} datasets to load into the ODS.")

        # --- 4. Process and Load Each Dataset using Enhanced COPY INTO ---
        total_rows_loaded = 0
        successful_loads = 0
        
        for s3_dataset_name, s3_key in latest_parquet_objects.items():
            ods_table_name = S3_TO_ODS_TABLE_MAP.get(s3_dataset_name)

            if not ods_table_name:
                logger.warning(f"No ODS table mapping for dataset '{s3_dataset_name}'. Skipping.")
                continue

            logger.info(f"Processing: {s3_dataset_name} -> {ods_table_name}")

            try:
                # Use enhanced COPY INTO to load data directly from S3
                copy_data_from_s3_to_snowflake(conn, s3_key, ods_table_name)
                
                # Validate the load
                row_count = validate_data_load(conn, ods_table_name)
                total_rows_loaded += row_count
                successful_loads += 1
                
            except Exception as e:
                logger.error(f"Failed to load data for {s3_dataset_name}: {e}")
                continue
        
        logger.info(f"--- Load Summary ---")
        logger.info(f"Successful loads: {successful_loads}/{len(latest_parquet_objects)}")
        logger.info(f"Total rows loaded: {total_rows_loaded}")
            
    except Exception as e:
        logger.error(f"An error occurred in the main process: {e}")
        raise
            
    finally:
        # --- 5. Close connection ---
        if conn:
            conn.close()
            logger.info("Snowflake connection closed.")
            
    logger.info("--- Enhanced ODS Ingestion Script Finished ---")

if __name__ == "__main__":
    main()