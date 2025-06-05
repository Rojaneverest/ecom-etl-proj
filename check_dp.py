import pandas as pd
import os
import unicodedata


def check_geolocation_duplicates(file_path):
    """
    Checks for duplicate entries in the geolocation dataset based on ALL columns.

    Args:
        file_path (str): The path to the olist_geolocation_dataset.csv file.
    """
    if not os.path.exists(file_path):
        print(f"Error: File not found at '{file_path}'")
        return

    print(f"Loading data from '{file_path}'...")
    try:
        df = pd.read_csv(file_path, low_memory=False)
        print(f"Successfully loaded {len(df)} rows.")
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return

    # Normalize column names to lowercase for consistency
    df.columns = [col.lower() for col in df.columns]

    # --- Pre-processing for consistency before duplicate check ---
    # Apply the same pre-processing as in s3_to_pg.py to ensure an accurate check
    if "geolocation_zip_code_prefix" in df.columns:
        df["geolocation_zip_code_prefix"] = (
            df["geolocation_zip_code_prefix"]
            .astype(str)
            .apply(
                lambda x: (
                    x.replace(".0", "")
                    if isinstance(x, str) and x.endswith(".0")
                    else x
                )
            )
        )
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
    # Ensure latitude and longitude are consistent types (e.g., float)
    if "geolocation_lat" in df.columns:
        df["geolocation_lat"] = pd.to_numeric(df["geolocation_lat"], errors="coerce")
    if "geolocation_lng" in df.columns:
        df["geolocation_lng"] = pd.to_numeric(df["geolocation_lng"], errors="coerce")

    # Check for duplicates based on ALL columns
    print("\nChecking for duplicates based on ALL columns (exact row duplicates)...")
    # `keep=False` marks all occurrences of a duplicate set as True
    duplicates_mask = df.duplicated(keep=False)
    duplicate_rows = df[duplicates_mask]

    num_exact_duplicates = len(duplicate_rows)
    num_unique_exact_duplicates = len(
        df[df.duplicated(keep="first")]
    )  # Count how many rows would be removed if we kept 'first'

    if num_exact_duplicates > 0:
        print(
            f"Found {num_exact_duplicates} rows that are exact duplicates of other rows."
        )
        print(
            f"This means {num_unique_exact_duplicates} rows would be removed if only one copy of each exact duplicate was kept."
        )

        print("\n--- Examples of exact duplicate sets (first 5 sets): ---")
        # Get unique duplicate rows to show representative examples
        unique_duplicate_sets = df[df.duplicated(keep="first")].drop_duplicates(
            keep="first"
        )

        count = 0
        for _, row in unique_duplicate_sets.head(5).iterrows():
            if count >= 5:
                break
            # Find all rows that match this unique duplicate set
            matching_rows = df[
                (df == row).all(axis=1)  # Match all columns for this specific row
            ]
            print(f"\nExample Duplicate Set {count + 1}:")
            print(matching_rows.to_string(index=False))
            print("-" * 30)
            count += 1
    else:
        print("No exact duplicate rows found across all columns.")


if __name__ == "__main__":
    geolocation_file_path = "data/extracted_data/olist_geolocation_dataset.csv"
    check_geolocation_duplicates(geolocation_file_path)
