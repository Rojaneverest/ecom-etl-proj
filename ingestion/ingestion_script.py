#!/usr/bin/env python3
"""
E-commerce Data Ingestion Script
--------------------------------
This script ingests CSV files containing e-commerce data from a local directory
and uploads them to an AWS S3 bucket, performing basic validation checks.
"""

import os
import sys
import csv
import logging
import boto3
import pandas as pd
from datetime import datetime
from botocore.exceptions import ClientError
import hashlib
import json
from io import BytesIO
import cProfile
import pstats

import unicodedata  # For removing accents


# Define the path to the data directory (no need to pass as argument)
DATA_DIR = "/Users/rojan/Desktop/cp-project/e-commerce-analytics/data/extracted_data"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("data_ingestion")

# Add file handler for persistent logging
log_file_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data_ingestion.log"
)
file_handler = logging.FileHandler(log_file_path)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)

# AWS Configuration - Replace with your own or use environment variables
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET = os.environ.get("S3_BUCKET", "your-e-commerce-data-lake")
RAW_DATA_PREFIX = "raw/"
QUARANTINE_PREFIX = "quarantine/"

# Data schema definitions based on the database diagram
SCHEMAS = {
    "olist_orders_dataset": {
        "required_fields": ["order_id", "customer_id"],
        "field_types": {
            "order_id": str,
            "customer_id": str,
            "order_status": str,
            "order_purchase_timestamp": str,  # Will be validated as datetime
            "order_approved_at": str,
            "order_delivered_carrier_date": str,
            "order_delivered_customer_date": str,
            "order_estimated_delivery_date": str,
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
            "shipping_limit_date": str,  # Will be validated as datetime
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
            "review_creation_date": str,  # Will be validated as datetime
            "review_answer_timestamp": str,  # Will be validated as datetime
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
        "field_types": {
            "id": int,
            "customer_id": str,
            "product_id": str,
            "wishlisted_at": str,  # Will be validated as datetime
        },
    },
    "product_views": {
        "required_fields": ["id", "customer_id", "product_id"],
        "field_types": {
            "id": int,
            "customer_id": str,
            "product_id": str,
            "viewed_at": str,  # Will be validated as datetime
        },
    },
    "add_to_carts": {
        "required_fields": ["id", "customer_id", "product_id"],
        "field_types": {
            "id": int,
            "customer_id": str,
            "product_id": str,
            "added_at": str,  # Will be validated as datetime
        },
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
    """Create and return an S3 client."""
    try:
        return boto3.client("s3", region_name=AWS_REGION)
    except Exception as e:
        logger.error(f"Failed to create S3 client: {e}")
        sys.exit(1)


def validate_timestamp(timestamp_str):
    """Validate if a string is a valid timestamp."""
    if (
        not timestamp_str
        or timestamp_str.lower() == "null"
        or timestamp_str.strip() == ""
    ):
        return True  # Allow empty timestamps

    for fmt in TIMESTAMP_FORMATS:
        try:
            datetime.strptime(timestamp_str, fmt)
            return True
        except ValueError:
            continue

    return False


def normalize_string(value):
    """Normalize a string to lowercase and remove accents."""
    if not isinstance(value, str):
        return value
    value = (
        unicodedata.normalize("NFKD", value).encode("ASCII", "ignore").decode("utf-8")
    )
    return value.lower().strip()


def validate_record(record, dataset_name):
    """
    Validate a record against its schema definition.
    Returns (is_valid, error_message)
    """
    if dataset_name not in SCHEMAS:
        return False, f"Unknown dataset: {dataset_name}"

    schema = SCHEMAS[dataset_name]

    # Cast zip_code_prefix to string for geolocation dataset
    if (
        dataset_name == "olist_geolocation_dataset"
        and "geolocation_zip_code_prefix" in record
    ):
        record["geolocation_zip_code_prefix"] = str(
            record["geolocation_zip_code_prefix"]
        )

    # Normalize city names
    if "geolocation_city" in record:
        record["geolocation_city"] = normalize_string(record["geolocation_city"])

    # Check required fields
    for field in schema["required_fields"]:
        if field not in record or not record[field]:
            return False, f"Missing required field: {field}"

    # Check field types and format
    for field, value in record.items():
        if field in schema["field_types"]:
            expected_type = schema["field_types"][field]

            # Skip validation for empty fields or None values
            if (
                value is None
                or value == ""
                or (isinstance(value, str) and value.strip() == "")
            ):
                continue

            # Validate timestamps
            if (
                field.endswith("_timestamp")
                or field.endswith("_date")
                or field.endswith("_at")
            ):
                if not validate_timestamp(str(value)):
                    return False, f"Invalid timestamp format for {field}: {value}"

            # Validate other types
            elif expected_type == int:
                try:
                    int(value)
                except (ValueError, TypeError):
                    return False, f"Field {field} should be integer, got: {value}"
            elif expected_type == float:
                try:
                    float(value)
                except (ValueError, TypeError):
                    return False, f"Field {field} should be float, got: {value}"
            elif expected_type == str and not isinstance(value, str):
                return (
                    False,
                    f"Field {field} should be string, got: {type(value).__name__}",
                )

    return True, ""


def check_duplicates(records, key_fields):
    """
    Check for duplicate records based on key fields.
    Returns a tuple of (unique_records, duplicate_records)
    """
    unique_records = []
    duplicate_records = []
    seen_keys = set()

    for record in records:
        # Normalize fields for hashing
        if "geolocation_city" in record:
            record["geolocation_city"] = normalize_string(record["geolocation_city"])

        # Create a hash key based on the key fields
        key_values = [str(record.get(field, "")) for field in key_fields]
        key = hashlib.md5("|".join(key_values).encode()).hexdigest()

        if key in seen_keys:
            duplicate_records.append(record)
        else:
            seen_keys.add(key)
            unique_records.append(record)

    return unique_records, duplicate_records


def safe_convert_numeric(df, field, expected_type):
    """Safely convert a column to numeric type with better error handling."""
    try:
        if field not in df.columns:
            return

        # Replace empty strings and whitespace-only strings with NaN first
        df[field] = df[field].replace(r"^\s*$", pd.NA, regex=True)

        if expected_type == int:
            # Convert to numeric, coercing errors to NaN
            df[field] = pd.to_numeric(df[field], errors="coerce")
        elif expected_type == float:
            # Convert to numeric, coercing errors to NaN
            df[field] = pd.to_numeric(df[field], errors="coerce")

    except Exception as e:
        logger.warning(
            f"Error converting column {field} to {expected_type.__name__}: {e}"
        )
        # Leave the column as-is if conversion fails


def process_csv_file(file_path, s3_client):
    """
    Process a CSV file:
    1. Validate schema
    2. Handle geolocation dataset separately
    3. Check for duplicates for other datasets
    4. Upload valid records to S3
    5. Quarantine invalid records
    """
    file_name = os.path.basename(file_path)
    dataset_name = os.path.splitext(file_name)[0]

    logger.info(f"Processing {file_name}...")

    if dataset_name not in SCHEMAS:
        logger.error(f"Unknown dataset: {dataset_name}, skipping file")
        return False

    try:
        # Read CSV with pandas for better handling of different formats
        # Use keep_default_na=False to prevent pandas from converting strings to NaN
        df = pd.read_csv(
            file_path, low_memory=False, keep_default_na=False, na_values=[]
        )
        schema = SCHEMAS[dataset_name]

        # Safely convert numeric columns
        for field, expected_type in schema["field_types"].items():
            if field in df.columns and expected_type in [int, float]:
                safe_convert_numeric(df, field, expected_type)

        # Convert DataFrame to records, handling None/NaN values properly
        records = []
        for _, row in df.iterrows():
            record = {}
            for col in df.columns:
                value = row[col]
                # Keep NaN/None as None, don't convert to empty string for numeric fields
                if pd.isna(value):
                    record[col] = None
                else:
                    record[col] = value
            records.append(record)

        # Validate each record
        valid_records = []
        invalid_records = []

        for i, record in enumerate(records):
            is_valid, error_message = validate_record(record, dataset_name)
            if is_valid:
                valid_records.append(record)
            else:
                record["_error"] = error_message
                invalid_records.append(record)
                logger.warning(
                    f"Invalid record in {file_name}, row {i+2}: {error_message}"
                )

        # Handle geolocation dataset differently
        if dataset_name == "olist_geolocation_dataset":
            unique_records = valid_records
            duplicate_records = []  # No deduplication for geolocation
        else:
            # Check for duplicates among valid records
            key_fields = schema["required_fields"]
            unique_records, duplicate_records = check_duplicates(
                valid_records, key_fields
            )

            if duplicate_records:
                logger.warning(
                    f"Found {len(duplicate_records)} duplicate records in {file_name}"
                )
                for record in duplicate_records:
                    record["_error"] = "Duplicate record"
                    invalid_records.append(record)

        # Create DataFrames from record lists
        valid_df = pd.DataFrame(unique_records)
        invalid_df = pd.DataFrame(invalid_records) if invalid_records else None

        # Upload valid records to S3
        if not valid_df.empty:
            buffer = BytesIO()
            valid_df.to_parquet(buffer, index=False, engine="pyarrow")
            buffer.seek(0)

            s3_key = f"{RAW_DATA_PREFIX}{dataset_name}/{datetime.now().strftime('%Y-%m-%d')}/{file_name.replace('.csv', '.parquet')}"

            s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
            logger.info(
                f"Uploaded {len(unique_records)} valid records to s3://{S3_BUCKET}/{s3_key}"
            )

        # Quarantine invalid records
        if invalid_df is not None and not invalid_df.empty:
            csv_buffer = invalid_df.to_csv(index=False)
            s3_key = f"{QUARANTINE_PREFIX}{dataset_name}/{datetime.now().strftime('%Y-%m-%d')}/{file_name}"
            s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_buffer)
            logger.info(
                f"Quarantined {len(invalid_records)} invalid records to s3://{S3_BUCKET}/{s3_key}"
            )

        # Generate and upload report
        report = {
            "file_name": file_name,
            "dataset_name": dataset_name,
            "total_records": len(records),
            "valid_records": len(unique_records),
            "invalid_records": len(invalid_records),
            "duplicate_records": (
                len(duplicate_records)
                if dataset_name != "olist_geolocation_dataset"
                else 0
            ),
            "timestamp": datetime.now().isoformat(),
        }

        s3_key = f"reports/ingestion_reports/{dataset_name}/{datetime.now().strftime('%Y-%m-%d')}/report.json"
        s3_client.put_object(
            Bucket=S3_BUCKET, Key=s3_key, Body=json.dumps(report, indent=2)
        )

        return True

    except Exception as e:
        logger.error(f"Error processing {file_name}: {e}")
        return False


def ensure_s3_bucket_exists(s3_client):
    """Ensure the S3 bucket exists, create it if it doesn't."""
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
        logger.info(f"Bucket {S3_BUCKET} already exists")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            logger.info(f"Creating bucket {S3_BUCKET}")
            s3_client.create_bucket(Bucket=S3_BUCKET)
        else:
            logger.error(f"Error checking bucket: {e}")
            sys.exit(1)


def main():
    """Main function to run the data ingestion process."""

    # Use the predefined data directory path
    data_dir = DATA_DIR

    if not os.path.isdir(data_dir):
        logger.error(f"Directory does not exist: {data_dir}")
        sys.exit(1)

    # Create S3 client
    s3_client = create_s3_client()

    # Ensure S3 bucket exists
    ensure_s3_bucket_exists(s3_client)

    # Process all CSV files in the directory
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
        f"Data ingestion completed. Processed {success_count} out of {len(csv_files)} files successfully."
    )


if __name__ == "__main__":
    profiler = cProfile.Profile()
    profiler.enable()
    main()
    profiler.disable()
    stats = pstats.Stats(profiler).sort_stats("cumtime")
    stats.print_stats(20)  # Print top 20 functions by cumulative time
    # stats.dump_stats("profiling_report.prof") # Save to a file for more detailed analysis with snakeviz
