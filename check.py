import snowflake.connector

# Snowflake connection details
SNOWFLAKE_USER = "ROJAN19"
SNOWFLAKE_PASSWORD = "e!Mv5ashy5aVdNb"
SNOWFLAKE_ACCOUNT = "TSSWLYO-FXC12703"
SNOWFLAKE_ROLE = "ACCOUNTADMIN"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DWH_DB = "ECOM_DWH"
SNOWFLAKE_DWH_SCHEMA = "PUBLIC"

# Define the tables in the ECOM_DWH database
# The order here is important if you were to create them, but for fetching, it doesn't strictly matter for independent fetches.
DWH_TABLES = [
    "DWH_GEOLOCATION",
    "DWH_PRODUCTS",
    "DWH_CUSTOMERS",
    "DWH_SELLERS",
    "DWH_ORDERS",
    "DWH_ORDER_ITEMS",
    "DWH_ORDER_PAYMENTS",
    "DWH_ORDER_REVIEWS",
]

def fetch_one_row_from_each_table():
    """
    Connects to Snowflake and fetches 1 row of data from each table in ECOM_DWH.
    """
    conn = None
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            role=SNOWFLAKE_ROLE,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DWH_DB,
            schema=SNOWFLAKE_DWH_SCHEMA
        )
        cursor = conn.cursor()

        print(f"Connected to Snowflake database: {SNOWFLAKE_DWH_DB}.{SNOWFLAKE_DWH_SCHEMA}")

        for table_name in DWH_TABLES:
            try:
                query = f"SELECT * FROM {SNOWFLAKE_DWH_DB}.{SNOWFLAKE_DWH_SCHEMA}.{table_name} LIMIT 1"
                cursor.execute(query)
                row = cursor.fetchone()

                print(f"\n--- Data from {table_name} ---")
                if row:
                    # Get column names from the cursor description
                    columns = [col[0] for col in cursor.description]
                    print(f"Columns: {columns}")
                    print(f"Row: {row}")
                else:
                    print(f"No data found in {table_name}.")
            except Exception as e:
                print(f"Error fetching data from {table_name}: {e}")

    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
    finally:
        if conn:
            conn.close()
            print("\nSnowflake connection closed.")

if __name__ == "__main__":
    fetch_one_row_from_each_table()