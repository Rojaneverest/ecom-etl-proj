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
# DATA_DIR = "/Users/rojan/Desktop/cp-project/e-commerce-analytics/data/extracted_data"
DATA_DIR = "/opt/airflow/data/extracted_data"
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
    """Create and return an S3 client with proper credential handling."""
    try:
        # First, try using environment variables or hardcoded credentials
        if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
            logger.info("Using provided AWS credentials")
            return boto3.client(
                "s3",
                region_name=AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            )
        else:
            # Fall back to default credential chain (AWS CLI, IAM roles, etc.)
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
            logger.error(
                "Access denied. Please check your AWS credentials and permissions."
            )
        elif error_code == "InvalidAccessKeyId":
            logger.error("Invalid access key ID. Please check your AWS credentials.")
        elif error_code == "SignatureDoesNotMatch":
            logger.error(
                "Invalid secret access key. Please check your AWS credentials."
            )
        else:
            logger.error(f"AWS S3 connection error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error testing S3 connection: {e}")
        return False


def validate_timestamp(timestamp_str):
    """
    Validates a timestamp string and normalizes it.
    - Returns a datetime object if the format is valid.
    - Returns None for empty or null-like strings.
    - Raises ValueError for invalid, non-empty formats.
    """
    if not timestamp_str or timestamp_str.strip().lower() in ["", "null"]:
        return None  # Normalize empty/null strings to None

    for fmt in TIMESTAMP_FORMATS:
        try:
            # Return the datetime object upon successful parsing
            return datetime.strptime(timestamp_str, fmt)
        except (ValueError, TypeError):
            continue

    raise ValueError(f"Invalid timestamp format: {timestamp_str}") 


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

            # In the validate_record function in your ingestion script

# Validate timestamps
            if (
                field.endswith("_timestamp")
                or field.endswith("_date")
                or field.endswith("_at")
            ):
                try:
                    # Use the function to get a normalized value (datetime object or None)
                    normalized_value = validate_timestamp(str(value))

                    # --- THIS IS THE FIX ---
                    # If the value is a datetime object, convert it back to an ISO 8601 string.
                    # If it's None, it remains None, which Pandas handles correctly as a null.
                    if isinstance(normalized_value, datetime):
                        record[field] = normalized_value.isoformat()
                    else:
                        record[field] = None # Ensure empty/invalid values are explicitly None

                except ValueError as e:
                    # The function will raise an error for invalid formats
                    return False, str(e)

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

        logger.debug(f"Converting field {field} to {expected_type.__name__}")
        
        # Make a copy to avoid modifying the original
        original_values = df[field].copy()
        
        # Replace empty strings and whitespace-only strings with NaN first
        df[field] = df[field].astype(str).replace(r"^\s*$", "", regex=True)
        df[field] = df[field].replace("", pd.NA)
        
        # Additional check: if this looks like a timestamp field, skip it
        # This is a safety net even though we filter before calling this function
        if (field.endswith("_timestamp") or field.endswith("_date") or field.endswith("_at")):
            logger.warning(f"Field {field} looks like a timestamp but was passed to numeric conversion. Skipping.")
            df[field] = original_values  # Restore original values
            return
        
        # Check if any values look like timestamps (contain colons or dashes in date format)
        sample_values = df[field].dropna().astype(str).head(10)
        for val in sample_values:
            if ":" in val or (len(val) >= 8 and val.count("-") >= 2):
                logger.warning(f"Field {field} contains timestamp-like values (e.g., '{val}'). Skipping numeric conversion.")
                df[field] = original_values  # Restore original values
                return

        if expected_type == int:
            # Convert to numeric, coercing errors to NaN
            df[field] = pd.to_numeric(df[field], errors="coerce", downcast="integer")
        elif expected_type == float:
            # Convert to numeric, coercing errors to NaN
            df[field] = pd.to_numeric(df[field], errors="coerce", downcast="float")
            
        logger.debug(f"Successfully converted {field} to {expected_type.__name__}")

    except Exception as e:
        logger.error(f"Error converting column {field} to {expected_type.__name__}: {e}")
        logger.error(f"Sample values: {df[field].head(5).tolist() if field in df.columns else 'Column not found'}")
        # Restore original values on error
        if 'original_values' in locals():
            df[field] = original_values


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
        # Read CSV with more explicit handling
        df = pd.read_csv(
            file_path, 
            low_memory=False, 
            keep_default_na=False, 
            na_values=[],
            dtype=str  # Read everything as string first
        )
        
        logger.info(f"Read {len(df)} rows from {file_name}")
        logger.debug(f"Columns in {file_name}: {list(df.columns)}")
        
        schema = SCHEMAS[dataset_name]

        # Apply safe_convert_numeric only to non-timestamp fields
        for field, expected_type in schema["field_types"].items():
            if field in df.columns and expected_type in [int, float]:
                # Double-check: is this a timestamp field?
                is_timestamp_field = (
                    field.endswith("_timestamp") or 
                    field.endswith("_date") or 
                    field.endswith("_at")
                )
                
                if not is_timestamp_field:
                    logger.debug(f"Applying numeric conversion to {field} (type: {expected_type.__name__})")
                    safe_convert_numeric(df, field, expected_type)
                else:
                    logger.debug(f"Skipping numeric conversion for timestamp field: {field}")

        # Convert DataFrame to records
        records = []
        for _, row in df.iterrows():
            record = {}
            for col in df.columns:
                value = row[col]
                if pd.isna(value) or value == "":
                    record[col] = None
                else:
                    record[col] = value
            records.append(record)

        valid_records = []
        invalid_records = []

        for i, record in enumerate(records):
            is_valid, error_message = validate_record(record, dataset_name)
            if is_valid:
                valid_records.append(record)
            else:
                record["_error"] = error_message
                invalid_records.append(record)
                if i < 5:  # Only log first 5 invalid records to avoid spam
                    logger.warning(f"Invalid record in {file_name}, row {i+2}: {error_message}")

        logger.info(f"Validation complete: {len(valid_records)} valid, {len(invalid_records)} invalid")

        if dataset_name == "olist_geolocation_dataset":
            unique_records = valid_records
            duplicate_records = []
        else:
            key_fields = schema["required_fields"]
            unique_records, duplicate_records = check_duplicates(valid_records, key_fields)

            if duplicate_records:
                logger.warning(f"Found {len(duplicate_records)} duplicate records in {file_name}")
                for record in duplicate_records:
                    record["_error"] = "Duplicate record"
                    invalid_records.append(record)

        if not unique_records:
            logger.warning(f"No valid unique records found in {file_name} after processing.")
            return False

        # Convert valid records to DataFrame before uploading
        valid_df = pd.DataFrame(unique_records)

        # Upload valid records to S3
        if not valid_df.empty:
            buffer = BytesIO()
            valid_df.to_parquet(buffer, index=False, engine="pyarrow")
            buffer.seek(0)

            s3_key = f"{RAW_DATA_PREFIX}{dataset_name}/{datetime.now().strftime('%Y-%m-%d')}/{file_name.replace('.csv', '.parquet')}"

            s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
            logger.info(f"Uploaded {len(unique_records)} valid records to s3://{S3_BUCKET}/{s3_key}")

        # Quarantine invalid records
        if invalid_records:
            invalid_df = pd.DataFrame(invalid_records)
            csv_buffer = invalid_df.to_csv(index=False)
            s3_key = f"{QUARANTINE_PREFIX}{dataset_name}/{datetime.now().strftime('%Y-%m-%d')}/{file_name}"
            s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_buffer)
            logger.info(f"Quarantined {len(invalid_records)} invalid records to s3://{S3_BUCKET}/{s3_key}")

        # Generate and upload report
        report = {
            "file_name": file_name,
            "dataset_name": dataset_name,
            "total_records": len(records),
            "valid_records": len(unique_records),
            "invalid_records": len(invalid_records),
            "duplicate_records": (
                len(duplicate_records) if dataset_name != "olist_geolocation_dataset" else 0
            ),
            "timestamp": datetime.now().isoformat(),
        }

        s3_key = f"reports/ingestion_reports/{dataset_name}/{datetime.now().strftime('%Y-%m-%d')}/report.json"
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=json.dumps(report, indent=2))

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
                # For us-east-1, don't specify LocationConstraint
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
                f"Access denied to bucket {S3_BUCKET}. Please check your AWS credentials and permissions."
            )
            return False
        else:
            logger.error(f"Error checking bucket: {e}")
            return False


def main():
    """Main function to run the data ingestion process."""

    # Use the predefined data directory path
    data_dir = DATA_DIR

    if not os.path.isdir(data_dir):
        logger.error(f"Directory does not exist: {data_dir}")
        sys.exit(1)

    # Create S3 client
    s3_client = create_s3_client()

    # Test S3 connection first
    if not test_s3_connection(s3_client):
        logger.error(
            "Failed to connect to AWS S3. Please check your credentials and try again."
        )
        sys.exit(1)

    # Ensure S3 bucket exists
    if not ensure_s3_bucket_exists(s3_client):
        logger.error("Failed to ensure S3 bucket exists. Exiting.")
        sys.exit(1)

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
