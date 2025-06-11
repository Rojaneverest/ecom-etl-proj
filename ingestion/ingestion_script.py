#!/usr/bin/env python3
"""
E-commerce Data Ingestion Script (Vectorized)
---------------------------------------------
This script ingests CSV files containing e-commerce data from a local directory
and uploads them to an AWS S3 bucket, performing basic validation checks using
vectorized pandas operations for efficiency.
"""

import os
import sys
import logging
import boto3
import pandas as pd
from datetime import datetime
from botocore.exceptions import ClientError
import json
from io import BytesIO
import cProfile
import pstats

# Define the path to the data directory (no need to pass as argument)
# DATA_DIR = "/Users/rojan/Desktop/cp-project/e-commerce-analytics/data/extracted_data"
DATA_DIR = "/opt/airflow/data/extracted_data"
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("data_ingestion_vectorized")

# Add file handler for persistent logging
log_file_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data_ingestion.log"
)
file_handler = logging.FileHandler(log_file_path)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)

# AWS Configuration - Use environment variables for security
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "rj-ecom-etl")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

RAW_DATA_PREFIX = "raw/"
QUARANTINE_PREFIX = "quarantine/"

# Log configuration (without exposing sensitive data)
logger.info(f"AWS_REGION: {AWS_REGION}")
logger.info(f"S3_BUCKET: {S3_BUCKET}")
logger.info(f"AWS_ACCESS_KEY_ID: {'***' if AWS_ACCESS_KEY_ID else 'Not set'}")
logger.info(f"AWS_SECRET_ACCESS_KEY: {'***' if AWS_SECRET_ACCESS_KEY else 'Not set'}")

# Data schema definitions based on the database diagram
SCHEMAS = {
    "olist_orders_dataset": {
        "required_fields": ["order_id", "customer_id"],
        "field_types": {
            "order_id": str,
            "customer_id": str,
            "order_status": str,
            "order_purchase_timestamp": "datetime",
            "order_approved_at": "datetime",
            "order_delivered_carrier_date": "datetime",
            "order_delivered_customer_date": "datetime",
            "order_estimated_delivery_date": "datetime",
        },
    },
    "olist_customers_dataset": {
        "required_fields": ["customer_id", "customer_unique_id"],
        "field_types": {
            "customer_id": str,
            "customer_unique_id": str,
            "customer_zip_code_prefix": int,
            "customer_city": str,
            "customer_state": str,
        },
    },
    "olist_order_customer_dataset": {
        "required_fields": ["customer_id", "customer_unique_id"],
        "field_types": {
            "customer_id": str,
            "customer_unique_id": str,
            "customer_zip_code_prefix": int,
            "customer_city": str,
            "customer_state": str,
        },
    },
    "olist_order_items_dataset": {
        "required_fields": ["order_id", "order_item_id", "product_id", "seller_id"],
        "field_types": {
            "order_id": str,
            "order_item_id": int,
            "product_id": str,
            "seller_id": str,
            "shipping_limit_date": "datetime",
            "price": float,
            "freight_value": float,
        },
    },
    "olist_order_payments_dataset": {
        "required_fields": ["order_id", "payment_sequential"],
        "field_types": {
            "order_id": str,
            "payment_sequential": int,
            "payment_type": str,
            "payment_installments": int,
            "payment_value": float,
        },
    },
    "olist_products_dataset": {
        "required_fields": ["product_id"],
        "field_types": {
            "product_id": str,
            "product_category_name": str,
            "product_name_lenght": int,
            "product_description_lenght": int,
            "product_photos_qty": int,
            "product_weight_g": float,
            "product_length_cm": float,
            "product_height_cm": float,
            "product_width_cm": float,
        },
    },
    "olist_order_reviews_dataset": {
        "required_fields": ["review_id", "order_id"],
        "field_types": {
            "review_id": str,
            "order_id": str,
            "review_score": int,
            "review_comment_title": str,
            "review_comment_message": str,
            "review_creation_date": "datetime",
            "review_answer_timestamp": "datetime",
        },
    },
    "olist_sellers_dataset": {
        "required_fields": ["seller_id"],
        "field_types": {
            "seller_id": str,
            "seller_zip_code_prefix": int,
            "seller_city": str,
            "seller_state": str,
        },
    },
    "olist_geolocation_dataset": {
        "required_fields": ["geolocation_zip_code_prefix"],
        "field_types": {
            "geolocation_zip_code_prefix": int,
            "geolocation_lat": float,
            "geolocation_lng": float,
            "geolocation_city": str,
            "geolocation_state": str,
        },
    },
    "product_wishlists": {
        "required_fields": ["id", "customer_id", "product_id"],
        "field_types": {"id": int, "customer_id": str, "product_id": str, "wishlisted_at": "datetime"},
    },
    "product_views": {
        "required_fields": ["id", "customer_id", "product_id"],
        "field_types": {"id": int, "customer_id": str, "product_id": str, "viewed_at": "datetime"},
    },
    "add_to_carts": {
        "required_fields": ["id", "customer_id", "product_id"],
        "field_types": {"id": int, "customer_id": str, "product_id": str, "added_at": "datetime"},
    },
    "product_category_name_translation": {
        "required_fields": ["product_category_name"],
        "field_types": {
            "product_category_name": str,
            "product_category_name_english": str,
        },
    },
}

# Timestamp formats to validate
TIMESTAMP_FORMATS = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"]


def create_s3_client():
    """Create and return an S3 client with proper credential handling."""
    try:
        if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
            logger.info("Using provided AWS credentials")
            return boto3.client(
                "s3",
                region_name=AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            )
        else:
            logger.info("Using default AWS credential chain")
            return boto3.client("s3", region_name=AWS_REGION)
    except Exception as e:
        logger.error(f"Failed to create S3 client: {e}")
        sys.exit(1)


def test_s3_connection(s3_client):
    """Test S3 connection by listing buckets."""
    try:
        response = s3_client.list_buckets()
        logger.info("Successfully connected to AWS S3")
        logger.info(
            f"Available buckets: {[bucket['Name'] for bucket in response['Buckets']]}"
        )
        return True
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "AccessDenied":
            logger.error("Access denied. Please check your AWS credentials and permissions.")
        elif error_code == "InvalidAccessKeyId":
            logger.error("Invalid access key ID. Please check your AWS credentials.")
        elif error_code == "SignatureDoesNotMatch":
            logger.error("Invalid secret access key. Please check your AWS credentials.")
        else:
            logger.error(f"AWS S3 connection error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error testing S3 connection: {e}")
        return False


def process_csv_file(file_path, s3_client):
    """
    Process a CSV file using vectorized Pandas operations:
    1. Validate schema, types, and required fields.
    2. Check for duplicates.
    3. Upload valid records to S3 (Parquet format).
    4. Quarantine invalid/duplicate records (CSV format).
    """
    file_name = os.path.basename(file_path)
    dataset_name = os.path.splitext(file_name)[0]

    logger.info(f"Processing {file_name} with vectorized approach...")

    if dataset_name not in SCHEMAS:
        logger.error(f"Unknown dataset: {dataset_name}, skipping file")
        return False

    try:
        df = pd.read_csv(
            file_path,
            low_memory=False,
            na_values=["", "null", "NULL"],
            keep_default_na=False,
            dtype=str,
        )
        logger.info(f"Read {len(df)} rows from {file_name}")

        schema = SCHEMAS[dataset_name]
        initial_record_count = len(df)
        error_series = pd.Series(index=df.index, dtype=object)

        # --- 1. Vectorized Validation ---

        # a) Required Fields
        for field in schema["required_fields"]:
            if field in df.columns:
                mask = df[field].isnull() | (df[field].str.strip() == "")
                error_series.loc[mask] = error_series.loc[mask].fillna(
                    f"Missing required field: {field}"
                )
            else:
                error_series.fillna(
                    f"Missing required column: {field}", inplace=True
                )
                logger.error(f"Required column '{field}' not found in {file_name}.")
                break

        # b) String Normalization & Type/Format Validation
        for field, expected_type in schema["field_types"].items():
            if field not in df.columns:
                continue

            # Normalize city names (vectorized)
            if field == "geolocation_city":
                df[field] = (
                    df[field]
                    .str.normalize("NFKD")
                    .str.encode("ASCII", "ignore")
                    .str.decode("utf-8")
                    .str.lower()
                    .str.strip()
                )

            # Validate Datetimes (vectorized)
            if expected_type == "datetime":
                parsed_col = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
                to_parse_mask = df[field].notna()
                for fmt in TIMESTAMP_FORMATS:
                    if to_parse_mask.any():
                        attempt = pd.to_datetime(
                            df.loc[to_parse_mask, field], format=fmt, errors="coerce"
                        )
                        parsed_col.update(attempt.dropna())
                        to_parse_mask = parsed_col.isna() & df[field].notna()

                invalid_mask = to_parse_mask
                error_series.loc[invalid_mask] = error_series.loc[
                    invalid_mask
                ].fillna(f"Invalid timestamp format for field: {field}")
                df[field] = parsed_col.dt.strftime("%Y-%m-%d %H:%M:%S").replace(
                    {pd.NaT: None}
                )

            # Validate Numeric Types (vectorized)
            elif expected_type in [int, float]:
                original_col = df[field].copy()
                numeric_col = pd.to_numeric(df[field], errors="coerce")
                invalid_mask = numeric_col.isna() & original_col.notna()
                error_series.loc[invalid_mask] = error_series.loc[
                    invalid_mask
                ].fillna(f"Field {field} should be {expected_type.__name__}")
                df[field] = numeric_col

        # --- 2. Split DataFrame ---
        invalid_mask = error_series.notna()
        invalid_df = df[invalid_mask].copy()
        invalid_df["_error"] = error_series[invalid_mask]
        valid_df = df[~invalid_mask].copy()
        logger.info(f"Validation complete: {len(valid_df)} valid, {len(invalid_df)} invalid")

        # --- 3. Duplicate Checking (Vectorized) ---
        duplicate_records_df = pd.DataFrame()
        unique_df = valid_df
        if dataset_name != "olist_geolocation_dataset" and not valid_df.empty:
            key_fields = schema["required_fields"]
            duplicates_mask = valid_df.duplicated(subset=key_fields, keep="first")
            if duplicates_mask.any():
                duplicate_records_df = valid_df[duplicates_mask].copy()
                duplicate_records_df["_error"] = "Duplicate record"
                logger.warning(
                    f"Found {len(duplicate_records_df)} duplicate records in {file_name}"
                )
                invalid_df = pd.concat([invalid_df, duplicate_records_df], ignore_index=True)
                unique_df = valid_df[~duplicates_mask]

        if unique_df.empty:
            logger.warning(
                f"No valid unique records found in {file_name} after processing."
            )

        # --- 4. Upload to S3 ---
        if not unique_df.empty:
            buffer = BytesIO()
            unique_df.to_parquet(buffer, index=False, engine="pyarrow")
            buffer.seek(0)
            s3_key = f"{RAW_DATA_PREFIX}{dataset_name}/{datetime.now().strftime('%Y-%m-%d')}/{file_name.replace('.csv', '.parquet')}"
            s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
            logger.info(
                f"Uploaded {len(unique_df)} valid records to s3://{S3_BUCKET}/{s3_key}"
            )

        if not invalid_df.empty:
            csv_buffer = invalid_df.to_csv(index=False).encode("utf-8")
            s3_key = f"{QUARANTINE_PREFIX}{dataset_name}/{datetime.now().strftime('%Y-%m-%d')}/{file_name}"
            s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_buffer)
            logger.info(
                f"Quarantined {len(invalid_df)} invalid records to s3://{S3_BUCKET}/{s3_key}"
            )

        # --- 5. Generate and Upload Report ---
        report = {
            "file_name": file_name,
            "dataset_name": dataset_name,
            "total_records": initial_record_count,
            "valid_records": len(unique_df),
            "invalid_records": len(invalid_df) - len(duplicate_records_df),
            "duplicate_records": len(duplicate_records_df),
            "timestamp": datetime.now().isoformat(),
        }
        s3_key = f"reports/ingestion_reports/{dataset_name}/{datetime.now().strftime('%Y-%m-%d')}/report.json"
        s3_client.put_object(
            Bucket=S3_BUCKET, Key=s3_key, Body=json.dumps(report, indent=2).encode("utf-8")
        )

        return True

    except Exception as e:
        logger.error(f"Error processing {file_name}: {str(e)}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return False


def ensure_s3_bucket_exists(s3_client):
    """Ensure the S3 bucket exists, create it if it doesn't."""
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
        logger.info(f"Bucket {S3_BUCKET} already exists")
        return True
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            try:
                logger.info(f"Creating bucket {S3_BUCKET}")
                if AWS_REGION == "us-east-1":
                    s3_client.create_bucket(Bucket=S3_BUCKET)
                else:
                    s3_client.create_bucket(
                        Bucket=S3_BUCKET,
                        CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
                    )
                logger.info(f"Successfully created bucket {S3_BUCKET}")
                return True
            except ClientError as create_error:
                logger.error(f"Error creating bucket: {create_error}")
                return False
        elif error_code == "403":
            logger.error(
                f"Access denied to bucket {S3_BUCKET}. Check AWS credentials/permissions."
            )
            return False
        else:
            logger.error(f"Error checking bucket: {e}")
            return False


def main():
    """Main function to run the data ingestion process."""
    data_dir = DATA_DIR
    if not os.path.isdir(data_dir):
        logger.error(f"Directory does not exist: {data_dir}")
        sys.exit(1)

    s3_client = create_s3_client()
    if not test_s3_connection(s3_client):
        logger.error("Failed to connect to AWS S3. Check credentials and try again.")
        sys.exit(1)

    if not ensure_s3_bucket_exists(s3_client):
        logger.error("Failed to ensure S3 bucket exists. Exiting.")
        sys.exit(1)

    csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
    if not csv_files:
        logger.warning(f"No CSV files found in {data_dir}")
        sys.exit(0)

    logger.info(f"Found {len(csv_files)} CSV files to process")
    success_count = 0
    for file_name in csv_files:
        file_path = os.path.join(data_dir, file_name)
        if process_csv_file(file_path, s3_client):
            success_count += 1

    logger.info(
        f"Data ingestion completed. Processed {success_count} of {len(csv_files)} files successfully."
    )


if __name__ == "__main__":
    profiler = cProfile.Profile()
    profiler.enable()
    main()
    profiler.disable()
    stats = pstats.Stats(profiler).sort_stats("cumtime")
    stats.print_stats(20)