import os
import io
import sys
import boto3
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from botocore.exceptions import ClientError
from botocore.config import Config
import logging
import unicodedata
import datetime
import json
import cProfile
import pstats
import os

load_dotenv()

# --- Configuration ---
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
logger = logging.getLogger("s3_parquet_to_postgres_ingestion")

# Create local reports directory
REPORTS_DIR = "reports/processing_reports"
os.makedirs(REPORTS_DIR, exist_ok=True)

# Generate processing timestamp
PROCESSING_TIMESTAMP = datetime.datetime.now()
PROCESSING_DATE = PROCESSING_TIMESTAMP.strftime("%Y-%m-%d")

log_filename = os.path.join(
    REPORTS_DIR, f"processing_{PROCESSING_TIMESTAMP:%Y%m%d_%H%M%S}.log"
)
file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
file_handler.setFormatter(file_formatter)

logger.addHandler(file_handler)

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
    "product_wishlists": "product_wishlists",
    "product_views": "product_views",
    "add_to_carts": "add_to_carts",
}

# --- Reverse mapping for easier lookup ---
DB_TABLE_TO_S3_MAP = {v: k for k, v in S3_TO_DB_TABLE_MAP.items()}

# --- Define the correct loading order for tables to respect foreign key dependencies ---
TABLE_LOADING_ORDER = [
    "geolocation",
    "customers",
    "sellers",
    "products",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
]

# Global processing report
PROCESSING_REPORT = {
    "processing_timestamp": PROCESSING_TIMESTAMP.isoformat(),
    "processing_date": PROCESSING_DATE,
    "datasets_processed": {},
    "summary": {
        "total_datasets": 0,
        "successful_ingestions": 0,
        "failed_ingestions": 0,
        "total_rows_processed": 0,
    },
}

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


def get_latest_parquet_objects(s3_client, bucket, prefix):
    """
    Lists the latest Parquet files under each dataset directory within the raw prefix.
    Assumes structure like: raw/dataset_name/YYYY-MM-DD/filename.parquet
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
                        if obj["Key"].endswith(".parquet"):
                            latest_objects[dataset_name] = obj["Key"]
                            break

            if dataset_name not in latest_objects:
                logger.warning(
                    f"No Parquet found in the latest date folder for dataset: {dataset_name}"
                )

    except ClientError as e:
        logger.error(f"Error listing S3 objects: {e}")
        return {}
    return latest_objects


def download_s3_parquet_to_dataframe(s3_client, bucket, key):
    """Downloads a Parquet file from S3 and returns it as a pandas DataFrame."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        parquet_content = response["Body"].read()

        # Read Parquet data into DataFrame
        df = pd.read_parquet(io.BytesIO(parquet_content))
        logger.info(f"Downloaded s3://{bucket}/{key} with {len(df)} rows.")
        return df
    except ClientError as e:
        logger.error(f"Error downloading {key} from S3: {e}")
        return None
    except Exception as e:
        logger.error(f"Error reading Parquet {key} into DataFrame: {e}")
        return None


def save_processed_parquet_to_s3(
    s3_client, df, s3_dataset_name, bucket, processing_date
):
    """
    Saves processed DataFrame as Parquet to S3 in the specified structure.
    Path: processed/dataset_name/YYYY-MM-DD/dataset_name.parquet
    """
    try:
        # Create the S3 key for processed data
        s3_key = f"{PROCESSED_DATA_PREFIX}{s3_dataset_name}/{processing_date}/{s3_dataset_name}.parquet"

        # Convert DataFrame to Parquet in memory
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        parquet_buffer.seek(0)

        # Upload to S3
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
            ContentType="application/octet-stream",
        )

        logger.info(f"Successfully saved processed data to s3://{bucket}/{s3_key}")
        return s3_key
    except Exception as e:
        logger.error(f"Error saving processed Parquet to S3 for {s3_dataset_name}: {e}")
        return None


def save_processing_report_to_s3(
    s3_client, report_data, s3_dataset_name, bucket, processing_date
):
    """
    Saves processing report as JSON to S3 in the specified structure.
    Path: reports/processing_reports/dataset_name/YYYY-MM-DD/report.json
    """
    try:
        # Create the S3 key for the report
        s3_key = f"{REPORTS_PREFIX}{s3_dataset_name}/{processing_date}/report.json"

        # Convert report to JSON
        report_json = json.dumps(report_data, indent=2, default=str)

        # Upload to S3
        s3_client.put_object(
            Bucket=bucket, Key=s3_key, Body=report_json, ContentType="application/json"
        )

        logger.info(f"Successfully saved processing report to s3://{bucket}/{s3_key}")
        return s3_key
    except Exception as e:
        logger.error(f"Error saving processing report to S3 for {s3_dataset_name}: {e}")
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
                cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")

                # --- Prepare DataFrame for COPY ---
                # Convert datetime columns to PostgreSQL compatible string format
                for col in df.select_dtypes(
                    include=["datetime64[ns]", "datetime64[ns, UTC]"]
                ).columns:
                    df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

                # Handle integer-like columns that might need special treatment
                integer_cols = {
                    "customer_zip_code_prefix",
                    "seller_zip_code_prefix",
                    "product_name_lenght",
                    "product_description_lenght",
                    "product_photos_qty",
                    "payment_sequential",
                    "payment_installments",
                    "review_score",
                    "order_item_id",
                    "geolocation_id",
                }

                for col in integer_cols:
                    if col in df.columns:
                        # For Parquet data, handle type conversion carefully
                        if col in [
                            "customer_zip_code_prefix",
                            "seller_zip_code_prefix",
                        ]:
                            # These should be strings
                            df[col] = (
                                df[col].astype(str).str.replace(r"\.0$", "", regex=True)
                            )
                        else:
                            # Convert to nullable integer type
                            df[col] = pd.to_numeric(df[col], errors="coerce")
                            df[col] = df[col].astype(pd.Int64Dtype())

                # Convert DataFrame to CSV format for COPY
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, sep="\t", header=True, index=False, na_rep="")
                csv_buffer.seek(0)

                # --- Execute COPY command ---
                columns = ", ".join([f'"{col}"' for col in df.columns])
                copy_sql = f"COPY {table_name} ({columns}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER E'\\t', NULL '');"
                logger.info(f"Executing COPY for table '{table_name}'...")
                cursor.copy_expert(copy_sql, csv_buffer)
            raw_connection.commit()

        logger.info(f"Successfully ingested {len(df)} rows into '{table_name}'.")
        return True
    except Exception as e:
        logger.error(f"Error ingesting data into '{table_name}': {e}")
        return False


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
        df_geo_map["geolocation_zip_code_prefix"] = df_geo_map[
            "geolocation_zip_code_prefix"
        ].astype(str)
        logger.info(
            f"Fetched {len(df_geo_map)} unique zip code to geolocation_id mappings."
        )
        return df_geo_map
    except Exception as e:
        logger.error(f"Error fetching geolocation mapping: {e}")
        raise


def main():
    """Main function to run the S3 Parquet to PostgreSQL data ingestion."""
    s3_client = create_s3_client()
    engine = create_engine(DATABASE_URL)

    logger.info(
        f"Listing latest Parquet files from s3://{S3_BUCKET}/{RAW_DATA_PREFIX}..."
    )
    # Look for Parquet files in the raw prefix structure
    latest_parquet_objects = get_latest_parquet_objects(
        s3_client, S3_BUCKET, RAW_DATA_PREFIX
    )

    if not latest_parquet_objects:
        logger.warning("No latest Parquet objects found in S3 bucket. Exiting.")
        return

    logger.info(f"Found {len(latest_parquet_objects)} datasets to process.")
    PROCESSING_REPORT["summary"]["total_datasets"] = len(latest_parquet_objects)

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

        s3_key = latest_parquet_objects.get(s3_dataset_name)

        if s3_key:
            logger.info(
                f"Processing S3 key: {s3_key} -> Database table: {db_table_name}"
            )

            # Initialize dataset report
            dataset_report = {
                "dataset_name": s3_dataset_name,
                "database_table": db_table_name,
                "source_s3_key": s3_key,
                "processing_start_time": datetime.datetime.now().isoformat(),
                "original_row_count": 0,
                "processed_row_count": 0,
                "data_transformations": [],
                "ingestion_success": False,
                "processed_s3_key": None,
                "errors": [],
            }

            df = download_s3_parquet_to_dataframe(s3_client, S3_BUCKET, s3_key)
            if df is not None:
                dataset_report["original_row_count"] = len(df)

                # Normalize column names to lowercase to match database conventions
                df.columns = [col.lower() for col in df.columns]
                dataset_report["data_transformations"].append(
                    "Normalized column names to lowercase"
                )

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
                        dataset_report["data_transformations"].append(
                            "Converted zip code prefix to string format"
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
                        dataset_report["data_transformations"].append(
                            "Normalized city names (ASCII encoding, lowercase)"
                        )

                    # Remove duplicates based on composite primary key
                    original_rows = len(df)
                    df.drop_duplicates(
                        subset=[
                            "geolocation_zip_code_prefix",
                            "geolocation_lat",
                            "geolocation_lng",
                        ],
                        keep="first",
                        inplace=True,
                    )
                    if len(df) < original_rows:
                        duplicates_removed = original_rows - len(df)
                        logger.warning(
                            f"Removed {duplicates_removed} duplicate rows based on composite key from '{db_table_name}'."
                        )
                        dataset_report["data_transformations"].append(
                            f"Removed {duplicates_removed} duplicate rows based on composite key"
                        )

                    dataset_report["processed_row_count"] = len(df)

                    # Save processed data to S3
                    processed_s3_key = save_processed_parquet_to_s3(
                        s3_client, df, s3_dataset_name, S3_BUCKET, PROCESSING_DATE
                    )
                    dataset_report["processed_s3_key"] = processed_s3_key

                    # Ingest geolocation data first
                    ingestion_success = ingest_dataframe_to_postgres(
                        engine, df, db_table_name
                    )
                    dataset_report["ingestion_success"] = ingestion_success

                    if ingestion_success:
                        PROCESSING_REPORT["summary"]["successful_ingestions"] += 1
                        PROCESSING_REPORT["summary"]["total_rows_processed"] += len(df)
                        # After successful ingestion, create the mapping for customers/sellers
                        geolocation_id_map = get_geolocation_id_map(engine)
                    else:
                        PROCESSING_REPORT["summary"]["failed_ingestions"] += 1
                        dataset_report["errors"].append(
                            "Failed to ingest data into PostgreSQL"
                        )

                elif db_table_name in ["customers", "sellers"]:
                    if geolocation_id_map is None:
                        error_msg = f"Geolocation mapping is not available. Cannot process '{db_table_name}'. Skipping."
                        logger.error(error_msg)
                        dataset_report["errors"].append(error_msg)
                        PROCESSING_REPORT["summary"]["failed_ingestions"] += 1
                        continue

                    # Determine the zip code column name for the current table
                    zip_col_name = (
                        "customer_zip_code_prefix"
                        if db_table_name == "customers"
                        else "seller_zip_code_prefix"
                    )

                    # Ensure zip code columns are strings before merging
                    if zip_col_name in df.columns:
                        df[zip_col_name] = (
                            df[zip_col_name]
                            .astype(str)
                            .str.replace(r"\.0$", "", regex=True)
                        )
                        dataset_report["data_transformations"].append(
                            f"Converted {zip_col_name} to string format"
                        )

                    # Perform the merge to add geolocation_id
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
                    df["geolocation_id"] = df["geolocation_id"].astype(pd.Int64Dtype())
                    dataset_report["data_transformations"].append(
                        "Added geolocation_id mapping"
                    )

                    dataset_report["processed_row_count"] = len(df)

                    # Save processed data to S3
                    processed_s3_key = save_processed_parquet_to_s3(
                        s3_client, df, s3_dataset_name, S3_BUCKET, PROCESSING_DATE
                    )
                    dataset_report["processed_s3_key"] = processed_s3_key

                    # Ingest the processed DataFrame
                    ingestion_success = ingest_dataframe_to_postgres(
                        engine, df, db_table_name
                    )
                    dataset_report["ingestion_success"] = ingestion_success

                    if ingestion_success:
                        PROCESSING_REPORT["summary"]["successful_ingestions"] += 1
                        PROCESSING_REPORT["summary"]["total_rows_processed"] += len(df)
                    else:
                        PROCESSING_REPORT["summary"]["failed_ingestions"] += 1
                        dataset_report["errors"].append(
                            "Failed to ingest data into PostgreSQL"
                        )

                # Enhanced null handling for order_reviews table
                # Enhanced null handling for order_reviews table
                elif db_table_name == "order_reviews":

                    def clean_null_values(series, column_name):
                        """Clean various representations of null values in a pandas Series"""
                        # Convert to string first to handle all data types uniformly
                        series_str = series.astype(str)

                        # Define patterns that represent null values (matching CSV script logic)
                        null_patterns = [
                            "nan",
                            "none",
                            "null",
                            "[null]",
                            "[none]",
                            "na",
                            "<na>",
                            "",
                            "NULL",
                            "[NULL]",
                        ]

                        # Create mask for null-like values (case insensitive, matching CSV validation logic)
                        null_mask = (
                            series.isnull()
                            | (
                                series_str.str.lower()
                                .str.strip()
                                .isin([p.lower() for p in null_patterns])
                            )
                            | (
                                series_str.str.strip() == ""
                            )  # Handle empty strings after stripping
                        )

                        # Count nulls before replacement
                        null_count = null_mask.sum()

                        # Replace nulls with "None" (consistent with CSV script approach)
                        series.loc[null_mask] = "None"

                        logger.info(
                            f"Replaced {null_count} null/empty values with 'None' in {column_name} column."
                        )
                        return null_count

                    # Handle review comment title
                    if "review_comment_title" in df.columns:
                        null_count_title = clean_null_values(
                            df["review_comment_title"], "review_comment_title"
                        )
                        dataset_report["data_transformations"].append(
                            f"Replaced {null_count_title} null/empty values with 'None' in review_comment_title"
                        )

                    # Handle review comment message
                    if "review_comment_message" in df.columns:
                        null_count_message = clean_null_values(
                            df["review_comment_message"], "review_comment_message"
                        )
                        dataset_report["data_transformations"].append(
                            f"Replaced {null_count_message} null/empty values with 'None' in review_comment_message"
                        )

                    # Remove duplicate review_ids
                    original_rows = len(df)
                    df.drop_duplicates(subset=["review_id"], keep="first", inplace=True)
                    if len(df) < original_rows:
                        duplicates_removed = original_rows - len(df)
                        logger.warning(
                            f"Removed {duplicates_removed} duplicate review_id rows from '{db_table_name}'."
                        )
                        dataset_report["data_transformations"].append(
                            f"Removed {duplicates_removed} duplicate review_id rows"
                        )

                    dataset_report["processed_row_count"] = len(df)

                    # Save processed data to S3
                    processed_s3_key = save_processed_parquet_to_s3(
                        s3_client, df, s3_dataset_name, S3_BUCKET, PROCESSING_DATE
                    )
                    dataset_report["processed_s3_key"] = processed_s3_key

                    ingestion_success = ingest_dataframe_to_postgres(
                        engine, df, db_table_name
                    )
                    dataset_report["ingestion_success"] = ingestion_success

                    if ingestion_success:
                        PROCESSING_REPORT["summary"]["successful_ingestions"] += 1
                        PROCESSING_REPORT["summary"]["total_rows_processed"] += len(df)
                    else:
                        PROCESSING_REPORT["summary"]["failed_ingestions"] += 1
                        dataset_report["errors"].append(
                            "Failed to ingest data into PostgreSQL"
                        )

                else:  # For all other tables without special handling
                    dataset_report["processed_row_count"] = len(df)

                    # Save processed data to S3
                    processed_s3_key = save_processed_parquet_to_s3(
                        s3_client, df, s3_dataset_name, S3_BUCKET, PROCESSING_DATE
                    )
                    dataset_report["processed_s3_key"] = processed_s3_key

                    ingestion_success = ingest_dataframe_to_postgres(
                        engine, df, db_table_name
                    )
                    dataset_report["ingestion_success"] = ingestion_success

                    if ingestion_success:
                        PROCESSING_REPORT["summary"]["successful_ingestions"] += 1
                        PROCESSING_REPORT["summary"]["total_rows_processed"] += len(df)
                    else:
                        PROCESSING_REPORT["summary"]["failed_ingestions"] += 1
                        dataset_report["errors"].append(
                            "Failed to ingest data into PostgreSQL"
                        )

            else:
                error_msg = f"Failed to download or parse S3 object: {s3_key}"
                logger.error(error_msg)
                dataset_report["errors"].append(error_msg)
                PROCESSING_REPORT["summary"]["failed_ingestions"] += 1

            # Finalize dataset report
            dataset_report["processing_end_time"] = datetime.datetime.now().isoformat()

            # Save individual dataset report to S3
            save_processing_report_to_s3(
                s3_client, dataset_report, s3_dataset_name, S3_BUCKET, PROCESSING_DATE
            )

            # Add to main processing report
            PROCESSING_REPORT["datasets_processed"][s3_dataset_name] = dataset_report

        else:
            logger.warning(
                f"Latest Parquet object not found for S3 dataset: {s3_dataset_name}. Skipping '{db_table_name}'."
            )

    # Save overall processing report
    overall_report_key = (
        f"{REPORTS_PREFIX}overall_processing/{PROCESSING_DATE}/overall_report.json"
    )
    try:
        report_json = json.dumps(PROCESSING_REPORT, indent=2, default=str)
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=overall_report_key,
            Body=report_json,
            ContentType="application/json",
        )
        logger.info(
            f"Successfully saved overall processing report to s3://{S3_BUCKET}/{overall_report_key}"
        )
    except Exception as e:
        logger.error(f"Error saving overall processing report to S3: {e}")

    logger.info("S3 Parquet to PostgreSQL data ingestion process completed.")
    logger.info(
        f"Summary: {PROCESSING_REPORT['summary']['successful_ingestions']}/{PROCESSING_REPORT['summary']['total_datasets']} datasets processed successfully."
    )
    logger.info(
        f"Total rows processed: {PROCESSING_REPORT['summary']['total_rows_processed']}"
    )


if __name__ == "__main__":
    profiler = cProfile.Profile()
    profiler.enable()
    main()
    profiler.disable()
    stats = pstats.Stats(profiler).sort_stats("cumtime")
    stats.print_stats(20)  # Print top 20 functions by cumulative time
    # stats.dump_stats("profiling_report.prof") # Save to a file for more detailed analysis with snakeviz
