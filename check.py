import pandas as pd
import re
import logging

# --- Configuration ---
CSV_PATH = "data/extracted_data/olist_order_reviews_dataset.csv"
OUTPUT_INCONSISTENCY_CSV = "inconsistent_order_reviews.csv"

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def find_inconsistencies(csv_path: str, output_path: str):
    """
    Reads the olist_order_reviews_dataset.csv, identifies inconsistencies
    in 'order_id' (malformed format) and 'review_score' (non-integer values),
    and saves the inconsistent rows to a new CSV file.
    """
    try:
        # Read the CSV with low_memory=False to help with type inference
        # and ensure all columns are read correctly.
        # Assuming delimiter is comma. If not, add sep=';' or sep='\t'
        df = pd.read_csv(csv_path, low_memory=False)
        logger.info(f"Successfully read {len(df)} rows from {csv_path}")

        # Normalize column names to lowercase for easier access
        df.columns = [col.lower() for col in df.columns]

        inconsistent_rows = []

        # --- Check for order_id inconsistencies ---
        # A typical Olist order_id is a 32-character hexadecimal string (UUID-like)
        # We'll flag anything that doesn't match this pattern as potentially malformed.
        uuid_pattern = re.compile(r"^[a-f0-9]{32}$", re.IGNORECASE)

        for index, row in df.iterrows():
            is_inconsistent_row = False
            inconsistency_details = []

            order_id = row.get("order_id")
            if not isinstance(order_id, str) or not uuid_pattern.match(
                str(order_id).strip()
            ):
                inconsistency_details.append(
                    f"order_id='{order_id}' (not 32-char hex UUID-like)"
                )
                is_inconsistent_row = True

            # --- Check for review_score inconsistencies ---
            review_score = row.get("review_score")
            if pd.notna(review_score):  # Check if not NaN
                try:
                    # Attempt to convert to integer. If it fails, it's inconsistent.
                    int(review_score)
                except (ValueError, TypeError):
                    inconsistency_details.append(
                        f"review_score='{review_score}' (not an integer)"
                    )
                    is_inconsistent_row = True
            # else: # If review_score is NaN and you expect it to always be an integer
            #     inconsistency_details.append("review_score is NULL/NaN")
            #     is_inconsistent_row = True

            if is_inconsistent_row:
                # Create a copy to avoid SettingWithCopyWarning
                inconsistent_row = row.copy()
                inconsistent_row["inconsistency_reason"] = "; ".join(
                    inconsistency_details
                )
                inconsistent_rows.append(inconsistent_row)

        if inconsistent_rows:
            inconsistent_df = pd.DataFrame(inconsistent_rows)
            inconsistent_df.to_csv(output_path, index=False)
            logger.warning(
                f"Found {len(inconsistent_df)} inconsistent rows. Saved to {output_path}"
            )
        else:
            logger.info("No inconsistencies found based on defined criteria.")

    except FileNotFoundError:
        logger.error(f"Error: CSV file not found at {csv_path}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    find_inconsistencies(CSV_PATH, OUTPUT_INCONSISTENCY_CSV)
