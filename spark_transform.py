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

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    to_timestamp,
    coalesce,
    count,
    sum,
    explode,
    regexp_replace,
    lower,
    trim,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    TimestampType,
    LongType,
)
from pyspark.sql import Row


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
POSTGRES_JDBC_URL = "jdbc:postgresql://localhost:5432/cp_database"

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("s3_to_postgres_ingestion_pyspark")


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
    "product_category_name_translation": "product_category_name_translation",  # Included to explicitly skip in PySpark
    # Assuming these additional tables exist or will be created:
    "product_wishlists": "product_wishlists",
    "product_views": "product_views",
    "add_to_carts": "add_to_carts",
}

# --- Define the correct loading order for tables to respect foreign key dependencies ---
# This order is crucial for successful ingestion when dealing with FKs
TABLE_LOADING_ORDER = [
    "geolocation",
    "products",
    "customers",
    "sellers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    # Add any other tables that don't have dependencies on the above first
    # "product_wishlists",
    # "product_views",
    # "add_to_carts",
]

# --- PySpark Schemas for Each Dataset ---
# Define precise schemas to prevent inferSchema issues and ensure correct types
SPARK_SCHEMAS = {
    "olist_customers_dataset": StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("customer_unique_id", StringType(), True),
            StructField(
                "customer_zip_code_prefix", StringType(), True
            ),  # Read as String for join with geolocation
            StructField("customer_city", StringType(), True),
            StructField("customer_state", StringType(), True),
        ]
    ),
    "olist_sellers_dataset": StructType(
        [
            StructField("seller_id", StringType(), True),
            StructField(
                "seller_zip_code_prefix", StringType(), True
            ),  # Read as String for join with geolocation
            StructField("seller_city", StringType(), True),
            StructField("seller_state", StringType(), True),
        ]
    ),
    "olist_products_dataset": StructType(
        [
            StructField("product_id", StringType(), True),
            StructField("product_category_name", StringType(), True),
            StructField("product_name_lenght", IntegerType(), True),
            StructField("product_description_lenght", IntegerType(), True),
            StructField("product_photos_qty", IntegerType(), True),
            StructField("product_weight_g", FloatType(), True),
            StructField("product_length_cm", FloatType(), True),
            StructField("product_height_cm", FloatType(), True),
            StructField("product_width_cm", FloatType(), True),
        ]
    ),
    "olist_orders_dataset": StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_purchase_timestamp", TimestampType(), True),
            StructField("order_approved_at", TimestampType(), True),
            StructField("order_delivered_carrier_date", TimestampType(), True),
            StructField("order_delivered_customer_date", TimestampType(), True),
            StructField("order_estimated_delivery_date", TimestampType(), True),
        ]
    ),
    "olist_order_items_dataset": StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("order_item_id", IntegerType(), True),
            StructField("product_id", StringType(), True),
            StructField("seller_id", StringType(), True),
            StructField("shipping_limit_date", TimestampType(), True),
            StructField("price", FloatType(), True),
            StructField("freight_value", FloatType(), True),
        ]
    ),
    "olist_order_payments_dataset": StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("payment_sequential", IntegerType(), True),
            StructField("payment_type", StringType(), True),
            StructField("payment_installments", IntegerType(), True),
            StructField("payment_value", FloatType(), True),
        ]
    ),
    "olist_order_reviews_dataset": StructType(
        [
            StructField("review_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField(
                "review_score", IntegerType(), True
            ),  # <--- **THIS IS THE CRITICAL FIX**
            StructField("review_comment_title", StringType(), True),
            StructField("review_comment_message", StringType(), True),
            StructField("review_creation_date", TimestampType(), True),
            StructField("review_answer_timestamp", TimestampType(), True),
        ]
    ),
    "olist_geolocation_dataset": StructType(
        [
            StructField(
                "geolocation_zip_code_prefix", StringType(), True
            ),  # Read as String for consistency
            StructField("geolocation_lat", FloatType(), True),
            StructField("geolocation_lng", FloatType(), True),
            StructField("geolocation_city", StringType(), True),
            StructField("geolocation_state", StringType(), True),
        ]
    ),
    # Add schemas for other datasets if you process them
    # "product_wishlists": StructType([
    #     StructField("id", LongType(), True),
    #     StructField("customer_id", StringType(), True),
    #     StructField("product_id", StringType(), True),
    #     StructField("wishlisted_at", TimestampType(), True),
    # ]),
    # "product_views": StructType([
    #     StructField("id", LongType(), True),
    #     StructField("customer_id", StringType(), True),
    #     StructField("product_id", StringType(), True),
    #     StructField("viewed_at", TimestampType(), True),
    # ]),
    # "add_to_carts": StructType([
    #     StructField("id", LongType(), True),
    #     StructField("customer_id", StringType(), True),
    #     StructField("product_id", StringType(), True),
    #     StructField("added_at", TimestampType(), True),
    # ]),
}
# --- Functions (modified for PySpark) ---


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

        # Convert set to list and sort for consistent processing order if needed
        # dataset_prefixes = sorted(list(dataset_prefixes))

        logger.info(f"Found potential dataset folders in S3: {dataset_prefixes}")

        for dataset_name in dataset_prefixes:
            # Explicitly skip product_category_name_translation
            if dataset_name == "product_category_name_translation":
                logger.info(
                    f"Skipping processing for '{dataset_name}' as per configuration."
                )
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
                            # Extract date string from prefix, e.g., "raw/dataset/2023-01-01/" -> "2023-01-01"
                            date_str = common_date_prefix["Prefix"].split("/")[-2]
                            date_obj = pd.to_datetime(date_str)
                            date_folders[date_obj] = common_date_prefix["Prefix"]
                        except ValueError:
                            # Ignore prefixes that are not valid dates
                            continue

            if not date_folders:
                logger.warning(
                    f"No date folders found for dataset: {dataset_name}. Skipping."
                )
                continue

            latest_date = max(date_folders.keys())
            latest_date_folder_prefix = date_folders[latest_date]

            obj_paginator = s3_client.get_paginator("list_objects_v2")
            obj_pages = obj_paginator.paginate(
                Bucket=bucket, Prefix=latest_date_folder_prefix
            )
            csv_found = False
            for obj_page in obj_pages:
                if "Contents" in obj_page:
                    # Sort objects by LastModified to ensure truly latest if multiple CSVs exist
                    sorted_objects = sorted(
                        obj_page["Contents"],
                        key=lambda x: x["LastModified"],
                        reverse=True,
                    )
                    for obj in sorted_objects:
                        if obj["Key"].endswith(".csv"):
                            latest_objects[dataset_name] = obj["Key"]
                            logger.info(
                                f"Latest CSV for {dataset_name}: s3://{bucket}/{obj['Key']}"
                            )
                            csv_found = True
                            break  # Found the latest CSV for this dataset
                if csv_found:
                    break  # Break outer loop if CSV found for this dataset

            if dataset_name not in latest_objects:
                logger.warning(
                    f"No CSV found in the latest date folder for dataset: {dataset_name}. Skipping."
                )

    except ClientError as e:
        logger.error(
            f"Error listing S3 objects: {e}. Please check S3 bucket and prefix settings."
        )
        return {}
    except Exception as e:
        logger.error(f"An unexpected error occurred in get_latest_csv_objects: {e}")
        return {}
    return latest_objects


def ingest_df_to_postgres_pyspark(
    spark_df, db_table_name, jdbc_url, connection_properties
):
    """
    Ingests a PySpark DataFrame into a PostgreSQL table.
    This function assumes TRUNCATE TABLE ... CASCADE is handled externally (before calling this)
    to manage foreign key constraints.
    """
    logger.info(
        f"Ingesting processed data for '{db_table_name}' into PostgreSQL table '{db_table_name}'"
    )
    try:
        spark_df.write.jdbc(
            url=jdbc_url,
            table=db_table_name,
            mode="append",  # Use append mode because TRUNCATE is handled manually
            properties=connection_properties,
        )
        logger.info(
            f"Successfully ingested data for '{db_table_name}' into PostgreSQL."
        )
        return True
    except Exception as e:
        logger.error(f"Error ingesting data for '{db_table_name}' into PostgreSQL: {e}")
        return False


# --- NEW HELPER FUNCTION FOR GEOLOCATION MAPPING (using PySpark now) ---
def get_geolocation_id_map_pyspark(spark, jdbc_url, connection_properties):
    """
    Retrieves a deterministic mapping of geolocation_zip_code_prefix to a single geolocation_id
    from the database, using the lowest latitude as the tie-breaker, using PySpark.
    """
    logger.info("Fetching geolocation_id mapping from the database using PySpark...")
    query = """
    (SELECT geolocation_zip_code_prefix, geolocation_id
    FROM (
        SELECT geolocation_zip_code_prefix, geolocation_id,
               ROW_NUMBER() OVER (PARTITION BY geolocation_zip_code_prefix ORDER BY geolocation_lat ASC) as rn
        FROM geolocation
    ) AS sub
    WHERE rn = 1) AS geo_map
    """
    try:
        # Read directly into a PySpark DataFrame
        df_geo_map = spark.read.jdbc(
            url=jdbc_url,
            table=query,  # Use the subquery here
            properties=connection_properties,
        )
        # Ensure the zip code prefix is a string type in the DataFrame
        df_geo_map = df_geo_map.withColumn(
            "geolocation_zip_code_prefix",
            col("geolocation_zip_code_prefix").cast(StringType()),
        )
        logger.info(
            f"Fetched {df_geo_map.count()} unique zip code to geolocation_id mappings."
        )
        return df_geo_map
    except Exception as e:
        logger.error(f"Error fetching geolocation mapping with PySpark: {e}")
        raise  # Re-raise to stop if mapping cannot be created (critical for FKs)


def main():
    """Main function to run the S3 to PostgreSQL data ingestion using PySpark."""

    logger.info("Initializing SparkSession...")

    POSTGRES_JDBC_VERSION = "42.7.3"
    HADOOP_AWS_VERSION = "3.3.4"  # Matches Spark 3.5.1's bundled Hadoop
    AWS_SDK_BUNDLE_VERSION = "1.11.271"  # Specific version for Hadoop 3.3.4

    spark = (
        SparkSession.builder.appName("S3toPostgresPySpark")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.jars.packages",
            f"org.postgresql:postgresql:{POSTGRES_JDBC_VERSION},"
            f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION},"
            f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_BUNDLE_VERSION}",
        )
        .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential")
        .getOrCreate()
    )

    logger.info("SparkSession initialized.")
    logger.warning(
        "SECURITY WARNING: AWS credentials are hardcoded. This is not secure for production."
    )

    s3_client = create_s3_client()
    pg_engine = create_engine(DATABASE_URL)

    jdbc_connection_properties = {
        "user": "postgres",
        "password": "root",
        "driver": "org.postgresql.Driver",
    }

    logger.info(f"Listing latest CSVs from s3://{S3_BUCKET}/{RAW_DATA_PREFIX}...")
    latest_csv_objects = get_latest_csv_objects(s3_client, S3_BUCKET, RAW_DATA_PREFIX)

    if not latest_csv_objects:
        logger.warning("No latest CSV objects found in S3 bucket. Exiting.")
        spark.stop()
        return

    logger.info(f"Found {len(latest_csv_objects)} datasets to process.")

    tables_to_truncate = [
        "order_reviews",
        "order_payments",
        "order_items",
        "orders",
        "customers",
        "sellers",
        "products",
        "geolocation",
    ]

    with pg_engine.connect() as connection:
        raw_connection = connection.connection
        with raw_connection.cursor() as cursor:
            for table_name in tables_to_truncate:
                try:
                    logger.info(f"Truncating table '{table_name}' with CASCADE...")
                    cursor.execute(
                        f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;"
                    )
                    raw_connection.commit()
                    logger.info(f"Successfully truncated '{table_name}'.")
                except Exception as e:
                    logger.warning(
                        f"Could not truncate table '{table_name}' (might not exist or other issue): {e}"
                    )
                    raw_connection.rollback()

    geolocation_id_map_df = None

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
            s3_path = f"s3a://{S3_BUCKET}/{s3_key}"
            logger.info(f"Processing S3 path: {s3_path} -> DB table: {db_table_name}")

            try:
                # --- FIX APPLIED HERE: Use explicit schema for CSV reading ---
                if s3_dataset_name in SPARK_SCHEMAS:
                    logger.info(f"Applying explicit schema for {s3_dataset_name}.")
                    df = spark.read.csv(
                        s3_path,
                        header=True,
                        schema=SPARK_SCHEMAS[s3_dataset_name],
                        sep=",",
                        quote='"',
                        multiLine=True,
                        # Add option for date formats if TimestampType() doesn't parse correctly
                        # For example, if dates are "YYYY/MM/DD HH:MM:SS"
                        # dateFormat="yyyy/MM/dd", timestampFormat="yyyy/MM/dd HH:mm:ss"
                    )
                else:
                    logger.warning(
                        f"No explicit schema defined for {s3_dataset_name}. Inferring schema, which might be less robust."
                    )
                    df = spark.read.csv(
                        s3_path, header=True, inferSchema=True, multiLine=True
                    )

                logger.info(f"Read {df.count()} rows for {db_table_name}.")
                df.printSchema()  # Print the schema to confirm types

                # Normalize column names to lowercase to match database conventions
                df = df.toDF(*[col_name.lower() for col_name in df.columns])

                # --- Specific Data Cleaning/Transformations per table ---
                if db_table_name == "geolocation":
                    # Casting to StringType and regexp_replace for zip code prefix
                    if "geolocation_zip_code_prefix" in df.columns:
                        df = df.withColumn(
                            "geolocation_zip_code_prefix",
                            col("geolocation_zip_code_prefix").cast(StringType()),
                        )
                        df = df.withColumn(
                            "geolocation_zip_code_prefix",
                            regexp_replace(
                                col("geolocation_zip_code_prefix"), "\\.0$", ""
                            ),
                        )

                    # Normalize city names
                    if "geolocation_city" in df.columns:
                        df = df.withColumn(
                            "geolocation_city", lower(trim(col("geolocation_city")))
                        )
                        # Optional: Add UDF for full unicode normalization if needed.
                        # from pyspark.sql.functions import udf
                        # @udf(StringType())
                        # def normalize_city_udf(city_name):
                        #     if city_name is not None:
                        #         return unicodedata.normalize("NFKD", city_name).encode("ASCII", "ignore").decode("utf-8").lower().strip()
                        #     return None
                        # df = df.withColumn("geolocation_city", normalize_city_udf(col("geolocation_city")))

                    original_rows = df.count()
                    df = df.dropDuplicates(
                        [
                            "geolocation_zip_code_prefix",
                            "geolocation_lat",
                            "geolocation_lng",
                        ]
                    )
                    if df.count() < original_rows:
                        logger.info(
                            f"Removed {original_rows - df.count()} duplicate rows from '{db_table_name}'."
                        )

                    logger.info(
                        f"Saving processed data for '{db_table_name}' to s3a://{S3_BUCKET}/{PROCESSED_DATA_PREFIX}{db_table_name} (Parquet)"
                    )
                    df.coalesce(1).write.mode("overwrite").parquet(
                        f"s3a://{S3_BUCKET}/{PROCESSED_DATA_PREFIX}{db_table_name}"
                    )
                    logger.info(
                        f"Successfully saved processed data for '{db_table_name}' to S3."
                    )

                    ingest_df_to_postgres_pyspark(
                        df, db_table_name, POSTGRES_JDBC_URL, jdbc_connection_properties
                    )

                    geolocation_id_map_df = get_geolocation_id_map_pyspark(
                        spark, POSTGRES_JDBC_URL, jdbc_connection_properties
                    )

                    continue

                elif db_table_name in ["customers", "sellers"]:
                    if geolocation_id_map_df is None:
                        logger.error(
                            f"Geolocation mapping is not available. Cannot process '{db_table_name}'. Skipping."
                        )
                        continue

                    zip_col_name = (
                        "customer_zip_code_prefix"
                        if db_table_name == "customers"
                        else "seller_zip_code_prefix"
                    )

                    if zip_col_name in df.columns:
                        df = df.withColumn(
                            zip_col_name, col(zip_col_name).cast(StringType())
                        )
                        df = df.withColumn(
                            zip_col_name, regexp_replace(col(zip_col_name), "\\.0$", "")
                        )

                    df = df.join(
                        geolocation_id_map_df,
                        df[zip_col_name]
                        == geolocation_id_map_df["geolocation_zip_code_prefix"],
                        "left_outer",
                    ).drop(geolocation_id_map_df["geolocation_zip_code_prefix"])

                    df = df.withColumn(
                        "geolocation_id", col("geolocation_id").cast(LongType())
                    )

                    logger.info(
                        f"Saving processed data for '{db_table_name}' to s3a://{S3_BUCKET}/{PROCESSED_DATA_PREFIX}{db_table_name} (Parquet)"
                    )
                    df.coalesce(1).write.mode("overwrite").parquet(
                        f"s3a://{S3_BUCKET}/{PROCESSED_DATA_PREFIX}{db_table_name}"
                    )
                    logger.info(
                        f"Successfully saved processed data for '{db_table_name}' to S3."
                    )

                    ingest_df_to_postgres_pyspark(
                        df, db_table_name, POSTGRES_JDBC_URL, jdbc_connection_properties
                    )

                elif db_table_name == "order_reviews":
                    original_rows = df.count()
                    df = df.dropDuplicates(["review_id"])
                    if df.count() < original_rows:
                        logger.warning(
                            f"Removed {original_rows - df.count()} duplicate review_id rows from '{db_table_name}'."
                        )

                    # No further specific type casting is needed here IF the explicit schema works
                    # If 'review_score' is still an issue, you might need an additional .cast(IntegerType()) here
                    # df = df.withColumn("review_score", col("review_score").cast(IntegerType()))

                    logger.info(
                        f"Saving processed data for '{db_table_name}' to s3a://{S3_BUCKET}/{PROCESSED_DATA_PREFIX}{db_table_name} (Parquet)"
                    )
                    df.coalesce(1).write.mode("overwrite").parquet(
                        f"s3a://{S3_BUCKET}/{PROCESSED_DATA_PREFIX}{db_table_name}"
                    )
                    logger.info(
                        f"Successfully saved processed data for '{db_table_name}' to S3."
                    )

                    ingest_df_to_postgres_pyspark(
                        df, db_table_name, POSTGRES_JDBC_URL, jdbc_connection_properties
                    )

                else:
                    logger.info(
                        f"Saving processed data for '{db_table_name}' to s3a://{S3_BUCKET}/{PROCESSED_DATA_PREFIX}{db_table_name} (Parquet)"
                    )
                    df.coalesce(1).write.mode("overwrite").parquet(
                        f"s3a://{S3_BUCKET}/{PROCESSED_DATA_PREFIX}{db_table_name}"
                    )
                    logger.info(
                        f"Successfully saved processed data for '{db_table_name}' to S3."
                    )

                    ingest_df_to_postgres_pyspark(
                        df, db_table_name, POSTGRES_JDBC_URL, jdbc_connection_properties
                    )

            except Exception as e:
                logger.error(
                    f"Error processing table '{db_table_name}' from S3 key '{s3_key}': {e}"
                )
        else:
            logger.warning(
                f"Latest CSV object not found for S3 dataset: {s3_dataset_name}. Skipping '{db_table_name}'."
            )

    logger.info("S3 to PostgreSQL data ingestion process completed.")
    spark.stop()


if __name__ == "__main__":
    main()
