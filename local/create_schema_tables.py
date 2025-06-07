from sqlalchemy import create_engine, text


def create_tables_raw_sql(database_url):
    engine = create_engine(database_url)

    raw_sql_statements = """
    -- Drop tables in reverse order of dependency to avoid foreign key errors during recreation
    -- Added CASCADE to ensure dependent objects are also dropped
    DROP TABLE IF EXISTS order_reviews CASCADE;
    DROP TABLE IF EXISTS order_payments CASCADE;
    DROP TABLE IF EXISTS order_items CASCADE;
    DROP TABLE IF EXISTS orders CASCADE;
    DROP TABLE IF EXISTS customers CASCADE;
    DROP TABLE IF EXISTS products CASCADE;
    DROP TABLE IF EXISTS sellers CASCADE;
    DROP TABLE IF EXISTS geolocation CASCADE;


    CREATE TABLE geolocation (
        geolocation_id SERIAL PRIMARY KEY, -- Unique ID for each specific lat/lng pair for a zip
        geolocation_zip_code_prefix VARCHAR NOT NULL,
        geolocation_lat FLOAT NOT NULL,
        geolocation_lng FLOAT NOT NULL,
        geolocation_city VARCHAR,
        geolocation_state VARCHAR,
        -- Ensure that each combination of zip, lat, and lng is unique
        UNIQUE (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng)
    );

    CREATE TABLE customers (
        customer_id VARCHAR NOT NULL,
        customer_unique_id VARCHAR,
        customer_zip_code_prefix VARCHAR, -- Still keep this for easy reference
        customer_city VARCHAR,
        customer_state VARCHAR,
        geolocation_id INTEGER, -- New: Column to hold the FK to geolocation
        PRIMARY KEY (customer_id),
        -- New: Foreign Key to the new geolocation_id
        FOREIGN KEY (geolocation_id) REFERENCES geolocation (geolocation_id)
    );

    CREATE TABLE sellers (
        seller_id VARCHAR NOT NULL,
        seller_zip_code_prefix VARCHAR,
        seller_city VARCHAR,
        seller_state VARCHAR,
        geolocation_id INTEGER,  -- New: Column to hold the FK to geolocation
        PRIMARY KEY (seller_id),
        FOREIGN KEY (geolocation_id) REFERENCES geolocation (geolocation_id)
    );

    CREATE TABLE orders (
        order_id VARCHAR NOT NULL,
        customer_id VARCHAR,
        order_status VARCHAR,
        order_purchase_timestamp TIMESTAMP WITHOUT TIME ZONE,
        order_approved_at TIMESTAMP WITHOUT TIME ZONE,
        order_delivered_carrier_date TIMESTAMP WITHOUT TIME ZONE,
        order_delivered_customer_date TIMESTAMP WITHOUT TIME ZONE,
        order_estimated_delivery_date TIMESTAMP WITHOUT TIME ZONE,
        PRIMARY KEY (order_id),
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
    );

    CREATE TABLE products (
        product_id VARCHAR NOT NULL,
        product_category_name VARCHAR,
        product_name_lenght INTEGER,
        product_description_lenght INTEGER,
        product_photos_qty INTEGER,
        product_weight_g FLOAT,
        product_length_cm FLOAT,
        product_height_cm FLOAT,
        product_width_cm FLOAT,
        PRIMARY KEY (product_id)
    );



    CREATE TABLE order_items (
        order_id VARCHAR NOT NULL,
        order_item_id INTEGER NOT NULL,
        product_id VARCHAR,
        seller_id VARCHAR,
        shipping_limit_date TIMESTAMP WITHOUT TIME ZONE,
        price FLOAT,
        freight_value FLOAT,
        PRIMARY KEY (order_id, order_item_id),
        FOREIGN KEY (order_id) REFERENCES orders (order_id),
        FOREIGN KEY (product_id) REFERENCES products (product_id),
        FOREIGN KEY (seller_id) REFERENCES sellers (seller_id)
    );

    CREATE TABLE order_payments (
        order_id VARCHAR NOT NULL,
        payment_sequential INTEGER NOT NULL,
        payment_type VARCHAR,
        payment_installments INTEGER,
        payment_value FLOAT,
        PRIMARY KEY (order_id, payment_sequential),
        FOREIGN KEY (order_id) REFERENCES orders (order_id)
    );

    CREATE TABLE order_reviews (
        review_id VARCHAR NOT NULL,
        order_id VARCHAR,
        review_score INTEGER,
        review_comment_title VARCHAR,
        review_comment_message VARCHAR,
        review_creation_date TIMESTAMP WITHOUT TIME ZONE,
        review_answer_timestamp TIMESTAMP WITHOUT TIME ZONE,
        PRIMARY KEY (review_id),
        FOREIGN KEY (order_id) REFERENCES orders (order_id)
    );
    CREATE TABLE product_category_translation(
    product_category_name VARCHAR NOT NULL,
    product_category_name_english VARCHAR NOT NULL,
    PRIMARY KEY (product_category_name)
    )
    """

    with engine.connect() as connection:
        # Execute each statement
        for statement in raw_sql_statements.split(";"):
            stripped_statement = statement.strip()
            if stripped_statement:
                try:
                    connection.execute(text(stripped_statement))
                    print(f"Executed: {stripped_statement[:50]}...")
                except Exception as e:
                    print(
                        f"Error executing statement: {stripped_statement[:50]}...\nError: {e}"
                    )
        connection.commit()
    print("All raw SQL table creation statements executed successfully.")


# Example usage:
create_tables_raw_sql("postgresql://postgres:root@localhost:5432/cp_database")
