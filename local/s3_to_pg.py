import os
import io
import sys
import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from botocore.exceptions import ClientError
from botocore.config import Config
import logging
import unicodedata

# --- Configuration ---
# AWS S3 Configuration
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")
RAW_DATA_PREFIX = "raw/"
PROCESSED_DATA_PREFIX = "processed/"
REPORTS_PREFIX = "reports/processing_reports/"

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# PostgreSQL Database Configuration
DATABASE_URL = "postgresql://postgres:root@localhost:5432/cp_database"

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("s3_to_postgres_ingestion")

# --- Mapping S3 Object Names to Database Table Names ---
S3_TO_DB_TABLE_MAP = {
    "olist_customers_dataset": "customers",
    "olist_sellers_dataset": "sellers",
    "olist_products_dataset": "products",
    "olist_orders_dataset": "orders",
    "olist_order_items_dataset": "order_items",
    "olist_order_payments_dataset": "order_payments",
    "olist_order_reviews_dataset": "order_reviews",
    "olist_geolocation_dataset": "geolocation",
    "product_wishlists": "product_wishlists",  # Assuming these tables exist in DB if S3 files are found
    "product_views": "product_views",
    "add_to_carts": "add_to_carts",
}

# --- Define the correct loading order for tables to respect foreign key dependencies ---
# geolocation MUST be loaded before customers and sellers
TABLE_LOADING_ORDER = [
    "geolocation",  # Moved to the beginning
    "customers",
    "sellers",
    "products",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
]

# --- Functions ---


def create_s3_client():
    """Creates and returns an S3 client with increased timeout."""
    s3_config = Config(
        retries={"max_attempts": 10, "mode": "standard"},
        read_timeout=600,
        connect_timeout=60,
    )
    try:
        return boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            config=s3_config,
        )
    except Exception as e:
        logger.error(f"Failed to create S3 client: {e}")
        sys.exit(1)


def get_latest_csv_objects(s3_client, bucket, prefix):
    """
    Lists the latest CSV files under each dataset directory within a given prefix.
    Assumes structure like: prefix/dataset_name/YYYY-MM-DD/filename.csv
    Returns a dictionary mapping dataset_name to its latest S3 object key.
    """
    latest_objects = {}
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/")

        dataset_prefixes = set()
        for page in pages:
            if "CommonPrefixes" in page:
                for common_prefix in page["CommonPrefixes"]:
                    parts = common_prefix["Prefix"].split("/")
                    if len(parts) >= 2 and parts[1]:
                        dataset_prefixes.add(parts[1])

        for dataset_name in dataset_prefixes:
            # Skip product_category_name_translation if it's in the bucket
            if dataset_name == "product_category_name_translation":
                logger.info(f"Skipping processing for '{dataset_name}' as requested.")
                continue

            dataset_full_prefix = f"{prefix}{dataset_name}/"
            date_folders = {}
            date_paginator = s3_client.get_paginator("list_objects_v2")
            date_pages = date_paginator.paginate(
                Bucket=bucket, Prefix=dataset_full_prefix, Delimiter="/"
            )
            for date_page in date_pages:
                if "CommonPrefixes" in date_page:
                    for common_date_prefix in date_page["CommonPrefixes"]:
                        try:
                            date_str = common_date_prefix["Prefix"].split("/")[-2]
                            date_obj = pd.to_datetime(date_str)
                            date_folders[date_obj] = common_date_prefix["Prefix"]
                        except ValueError:
                            continue

            if not date_folders:
                logger.warning(f"No date folders found for dataset: {dataset_name}")
                continue

            latest_date_folder = date_folders[max(date_folders.keys())]

            obj_paginator = s3_client.get_paginator("list_objects_v2")
            obj_pages = obj_paginator.paginate(Bucket=bucket, Prefix=latest_date_folder)
            for obj_page in obj_pages:
                if "Contents" in obj_page:
                    for obj in obj_page["Contents"]:
                        if obj["Key"].endswith(".csv"):
                            latest_objects[dataset_name] = obj["Key"]
                            break

            if dataset_name not in latest_objects:
                logger.warning(
                    f"No CSV found in the latest date folder for dataset: {dataset_name}"
                )

    except ClientError as e:
        logger.error(f"Error listing S3 objects: {e}")
        return {}
    return latest_objects


def download_s3_csv_to_dataframe(s3_client, bucket, key):
    """Downloads a CSV from S3 and returns it as a pandas DataFrame."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        csv_content = response["Body"].read().decode("utf-8")
        df = pd.read_csv(
            io.StringIO(csv_content), low_memory=False
        )  # Use low_memory=False for better type inference
        logger.info(f"Downloaded s3://{bucket}/{key} with {len(df)} rows.")
        return df
    except ClientError as e:
        logger.error(f"Error downloading {key} from S3: {e}")
        return None
    except Exception as e:
        logger.error(f"Error reading CSV {key} into DataFrame: {e}")
        return None


def ingest_dataframe_to_postgres(engine, df, table_name):
    """
    Ingests a pandas DataFrame into a PostgreSQL table using COPY FROM STDIN.
    Includes TRUNCATE TABLE for idempotency and handles data type conversions.
    """
    if df is None or df.empty:
        logger.info(f"DataFrame for table '{table_name}' is empty. Skipping ingestion.")
        return False

    logger.info(f"Attempting to ingest {len(df)} rows into table '{table_name}'...")

    try:
        with engine.connect() as connection:
            raw_connection = connection.connection
            with raw_connection.cursor() as cursor:
                # --- Idempotency: TRUNCATE TABLE ---
                logger.info(f"Truncating table '{table_name}'...")
                # Use TRUNCATE ... CASCADE to handle foreign key dependencies
                cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")

                # --- Prepare DataFrame for COPY ---
                # Convert datetime columns to PostgreSQL compatible string format
                for col in df.select_dtypes(
                    include=["datetime64[ns]", "datetime64[ns, UTC]"]
                ).columns:
                    df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

                # Handle integer-like columns that might be read as floats (e.g., '40.0')
                integer_cols = {
                    "customer_zip_code_prefix",  # This column is VARCHAR in DB, but often read as int from CSV
                    "seller_zip_code_prefix",  # Same here
                    "product_name_lenght",
                    "product_description_lenght",
                    "product_photos_qty",
                    "payment_sequential",
                    "payment_installments",
                    "review_score",
                    "order_item_id",
                    "geolocation_id",  # Ensure geolocation_id is treated as integer
                }

                for col in integer_cols:
                    if col in df.columns:
                        # Convert to numeric, coercing unparseable values to NaN
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                        # Convert to nullable integer type (pandas.Int64Dtype) where appropriate
                        # Note: customer/seller zip prefixes should be strings for consistency with geolocation
                        if col in [
                            "customer_zip_code_prefix",
                            "seller_zip_code_prefix",
                        ]:
                            df[col] = (
                                df[col].astype(str).str.replace(r"\.0$", "", regex=True)
                            )  # Convert to string and remove .0
                        else:
                            df[col] = df[col].astype(pd.Int64Dtype())

                # Convert all columns to string representations suitable for COPY,
                # replacing pandas NaN/NaT with empty strings which 'NULL AS ''' handles.
                # For Int64Dtype, pd.NA values will automatically be converted to None by to_csv/StringIO
                # This ensures they are treated as NULL by COPY.
                csv_buffer = io.StringIO()
                df.to_csv(
                    csv_buffer, sep="\t", header=True, index=False, na_rep=""
                )  # na_rep="" for string/object cols, pd.NA handled for Int64Dtype
                csv_buffer.seek(0)  # Rewind the buffer to the beginning

                # --- Execute COPY command ---
                columns = ", ".join(
                    [f'"{col}"' for col in df.columns]
                )  # Quote column names for safety
                copy_sql = f"COPY {table_name} ({columns}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER E'\\t', NULL '');"
                logger.info(f"Executing COPY for table '{table_name}'...")
                cursor.copy_expert(copy_sql, csv_buffer)
            raw_connection.commit()  # Commit the transaction

        logger.info(f"Successfully ingested {len(df)} rows into '{table_name}'.")
        return True
    except Exception as e:
        logger.error(f"Error ingesting data into '{table_name}': {e}")
        return False


def save_dataframe_to_s3_as_parquet(s3_client, df, bucket, prefix, table_name):
    """
    Saves a pandas DataFrame to S3 in Parquet format.
    The 'coalesce(1)' equivalent is achieved by writing the entire DataFrame as one file.
    """
    if df is None or df.empty:
        logger.info(
            f"DataFrame for table '{table_name}' is empty. Skipping Parquet save to S3."
        )
        return False

    try:
        # Create a buffer to write the Parquet file to
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)  # Rewind the buffer

        # Define the S3 key for the Parquet file
        # Using a timestamp for uniqueness and partition-like structure if needed later
        # For 'coalesce(1)', we just save a single file for the entire table.
        s3_key = f"{prefix}{table_name}/{table_name}_latest.parquet"

        logger.info(f"Saving {len(df)} rows to s3://{bucket}/{s3_key} as Parquet...")
        s3_client.put_object(Bucket=bucket, Key=s3_key, Body=parquet_buffer.getvalue())
        logger.info(
            f"Successfully saved DataFrame for '{table_name}' to S3 as Parquet."
        )
        return True
    except ClientError as e:
        logger.error(f"Error saving Parquet for '{table_name}' to S3: {e}")
        return False
    except Exception as e:
        logger.error(
            f"An unexpected error occurred while saving Parquet for '{table_name}': {e}"
        )
        return False


# --- NEW HELPER FUNCTION FOR GEOLOCATION MAPPING ---
def get_geolocation_id_map(engine):
    """
    Retrieves a deterministic mapping of geolocation_zip_code_prefix to a single geolocation_id
    from the database, using the lowest latitude as the tie-breaker.
    """
    logger.info("Fetching geolocation_id mapping from the database...")
    query = """
    SELECT DISTINCT ON (geolocation_zip_code_prefix)
           geolocation_zip_code_prefix,
           geolocation_id
    FROM geolocation
    ORDER BY geolocation_zip_code_prefix, geolocation_lat ASC;
    """
    try:
        with engine.connect() as connection:
            df_geo_map = pd.read_sql(query, connection)
        # Ensure the zip code prefix is a string in the mapping DataFrame
        df_geo_map["geolocation_zip_code_prefix"] = df_geo_map[
            "geolocation_zip_code_prefix"
        ].astype(str)
        logger.info(
            f"Fetched {len(df_geo_map)} unique zip code to geolocation_id mappings."
        )
        return df_geo_map
    except Exception as e:
        logger.error(f"Error fetching geolocation mapping: {e}")
        raise  # Re-raise to stop if mapping cannot be created (critical for FKs)


def main():
    """Main function to run the S3 to PostgreSQL data ingestion and Parquet saving."""
    s3_client = create_s3_client()
    engine = create_engine(DATABASE_URL)

    logger.info(f"Listing latest CSVs from s3://{S3_BUCKET}/{RAW_DATA_PREFIX}...")
    latest_csv_objects = get_latest_csv_objects(s3_client, S3_BUCKET, RAW_DATA_PREFIX)

    if not latest_csv_objects:
        logger.warning("No latest CSV objects found in S3 bucket. Exiting.")
        return

    logger.info(f"Found {len(latest_csv_objects)} datasets to process.")

    # --- Initialize geolocation mapping outside the loop ---
    geolocation_id_map = None

    # --- Process tables in the defined loading order ---
    for db_table_name in TABLE_LOADING_ORDER:
        s3_dataset_name = next(
            (k for k, v in S3_TO_DB_TABLE_MAP.items() if v == db_table_name), None
        )

        if not s3_dataset_name:
            logger.warning(
                f"No S3 dataset found for database table '{db_table_name}' in S3_TO_DB_TABLE_MAP. Skipping."
            )
            continue

        s3_key = latest_csv_objects.get(s3_dataset_name)

        if s3_key:
            logger.info(
                f"Processing S3 key: {s3_key} -> Database table: {db_table_name}"
            )

            df = download_s3_csv_to_dataframe(s3_client, S3_BUCKET, s3_key)
            if df is not None:
                # Normalize column names to lowercase to match database conventions
                df.columns = [col.lower() for col in df.columns]

                # --- Specific Data Cleaning/Transformations per table ---
                if db_table_name == "geolocation":
                    # Convert zip code prefix to string and remove '.0' if present
                    if "geolocation_zip_code_prefix" in df.columns:
                        df["geolocation_zip_code_prefix"] = df[
                            "geolocation_zip_code_prefix"
                        ].astype(str)
                        df["geolocation_zip_code_prefix"] = df[
                            "geolocation_zip_code_prefix"
                        ].apply(
                            lambda x: (
                                x.replace(".0", "")
                                if isinstance(x, str) and x.endswith(".0")
                                else x
                            )
                        )
                    # Normalize city names
                    if "geolocation_city" in df.columns:
                        df["geolocation_city"] = df["geolocation_city"].apply(
                            lambda x: (
                                unicodedata.normalize("NFKD", str(x))
                                .encode("ASCII", "ignore")
                                .decode("utf-8")
                                .lower()
                                .strip()
                                if pd.notna(x)
                                else x
                            )
                        )
                    # --- Deduplicate based on composite primary key before ingestion ---
                    original_rows = len(df)
                    df.drop_duplicates(
                        subset=[
                            "geolocation_zip_code_prefix",
                            "geolocation_lat",
                            "geolocation_lng",
                        ],
                        keep="first",
                        inplace=True,  # Apply changes directly to df
                    )
                    if len(df) < original_rows:
                        logger.warning(
                            f"Removed {original_rows - len(df)} duplicate rows based on composite key from '{db_table_name}'."
                        )

                    # Ingest geolocation data first
                    ingest_dataframe_to_postgres(engine, df, db_table_name)
                    # After successful ingestion, create the mapping for customers/sellers
                    geolocation_id_map = get_geolocation_id_map(engine)

                    # Save to S3 as Parquet
                    save_dataframe_to_s3_as_parquet(
                        s3_client, df, S3_BUCKET, PROCESSED_DATA_PREFIX, db_table_name
                    )
                    continue  # Move to the next table in the loop

                elif db_table_name in ["customers", "sellers"]:
                    if geolocation_id_map is None:
                        logger.error(
                            f"Geolocation mapping is not available. Cannot process '{db_table_name}'. Skipping."
                        )
                        continue

                    # Determine the zip code column name for the current table
                    zip_col_name = (
                        "customer_zip_code_prefix"
                        if db_table_name == "customers"
                        else "seller_zip_code_prefix"
                    )

                    # --- CRITICAL FIX: Ensure zip code columns are strings before merging ---
                    if zip_col_name in df.columns:
                        df[zip_col_name] = (
                            df[zip_col_name]
                            .astype(str)
                            .str.replace(r"\.0$", "", regex=True)
                        )

                    # --- Perform the merge to add geolocation_id ---
                    # Use a left merge to keep all customers/sellers, even if their zip isn't in geolocation
                    df = pd.merge(
                        df,
                        geolocation_id_map,
                        left_on=zip_col_name,
                        right_on="geolocation_zip_code_prefix",
                        how="left",
                    )

                    # Drop the redundant zip code column from the merge
                    df.drop(columns=["geolocation_zip_code_prefix"], inplace=True)

                    # Ensure 'geolocation_id' column is of nullable integer type
                    # Values not found in mapping (NaN) will become pd.NA, which to_csv handles to NULL
                    df["geolocation_id"] = df["geolocation_id"].astype(pd.Int64Dtype())

                    # Ingest the processed DataFrame
                    ingest_dataframe_to_postgres(engine, df, db_table_name)

                    # Save to S3 as Parquet
                    save_dataframe_to_s3_as_parquet(
                        s3_client, df, S3_BUCKET, PROCESSED_DATA_PREFIX, db_table_name
                    )

                elif db_table_name == "order_reviews":

                    original_rows = len(df)
                    df.drop_duplicates(subset=["review_id"], keep="first", inplace=True)
                    if len(df) < original_rows:
                        logger.warning(
                            f"Removed {original_rows - len(df)} duplicate review_id rows from '{db_table_name}'."
                        )
                    ingest_dataframe_to_postgres(engine, df, db_table_name)
                    # Save to S3 as Parquet
                    save_dataframe_to_s3_as_parquet(
                        s3_client, df, S3_BUCKET, PROCESSED_DATA_PREFIX, db_table_name
                    )

                else:  # For all other tables without special handling
                    ingest_dataframe_to_postgres(engine, df, db_table_name)
                    # Save to S3 as Parquet
                    save_dataframe_to_s3_as_parquet(
                        s3_client, df, S3_BUCKET, PROCESSED_DATA_PREFIX, db_table_name
                    )
            else:
                logger.error(f"Failed to download or parse S3 object: {s3_key}")
        else:
            logger.warning(
                f"Latest CSV object not found for S3 dataset: {s3_dataset_name}. Skipping '{db_table_name}'."
            )

    logger.info("S3 to PostgreSQL data ingestion and Parquet saving process completed.")


if __name__ == "__main__":
    main()
