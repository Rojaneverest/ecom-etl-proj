# helpers.py
import streamlit as st
import snowflake.connector
import pandas as pd

# Use Streamlit's caching to prevent re-running expensive queries.
# The cache will be cleared when the query changes.
@st.cache_data
def run_query(query: str) -> pd.DataFrame:
    """
    Connects to Snowflake, executes a query, and returns the result as a Pandas DataFrame.
    The connection is automatically closed.
    """
    try:
        # Connect using credentials from st.secrets
        with snowflake.connector.connect(**st.secrets["snowflake"]) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                df = cur.fetch_pandas_all()
        return df
    except snowflake.connector.errors.ProgrammingError as e:
        st.error(f"Snowflake Programming Error: {e}")
        st.error("Please check if the tables and columns in your query exist and you have the correct permissions.")
        return pd.DataFrame() # Return empty DataFrame on error
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return pd.DataFrame()