from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.engine import reflection

# PostgreSQL Database Configuration
DATABASE_URL = "postgresql://postgres:root@localhost:5432/cp_database"


def drop_all_tables_cascade(database_url, schema="public"):
    """
    Connects to the PostgreSQL database and drops all tables within the specified schema,
    using CASCADE to remove dependent objects.
    """
    engine = create_engine(database_url)

    try:
        with engine.connect() as connection:
            # Get an Inspector to reflect the database schema
            inspector = reflection.Inspector.from_engine(engine)

            # Get the list of all table names in the specified schema
            # We exclude system tables by checking the schema name
            table_names = inspector.get_table_names(schema=schema)

            if not table_names:
                print(f"No tables found in schema '{schema}' to drop.")
                return

            print(f"Found {len(table_names)} tables in schema '{schema}' to drop.")

            # Iterate through the tables and drop them with CASCADE
            # It's often safer to reverse the order for dropping, though CASCADE handles most cases.
            # In PostgreSQL, dropping tables with CASCADE is generally handled robustly even with circular deps
            # because it determines the correct order internally.
            for table_name in reversed(table_names):  # Reverse order for safer dropping
                full_table_name = (
                    f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
                )
                drop_sql = f"DROP TABLE IF EXISTS {full_table_name} CASCADE;"
                print(f"Executing: {drop_sql}")
                connection.execute(text(drop_sql))

            connection.commit()
            print(f"Successfully dropped all tables in schema '{schema}' with CASCADE.")

    except Exception as e:
        print(f"An error occurred while dropping tables: {e}")


if __name__ == "__main__":
    # !!! WARNING: This will permanently delete all data and table structures. !!!
    # !!! Ensure you have backups or are absolutely sure before running this. !!!
    confirm = input(
        "Are you sure you want to DROP ALL TABLES with CASCADE? This cannot be undone! (yes/no): "
    )
    if confirm.lower() == "yes":
        drop_all_tables_cascade(DATABASE_URL)
    else:
        print("Table dropping cancelled.")
