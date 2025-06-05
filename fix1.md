# Data Ingestion Script - Numeric Conversion Issue Documentation

## Problem Description

The data ingestion script was failing when processing the `olist_products_dataset.csv` file with the following error:

```
2025-06-05 11:48:13,441 - data_ingestion - ERROR - Error processing olist_products_dataset.csv: 
("Could not convert '' with type str: tried to convert to double", 
'Conversion failed for column product_name_lenght with type object')
```

## Root Cause Analysis

### The Issue Chain

1. **CSV Data Reality**: The products dataset contained empty cells for some numeric columns (like `product_name_lenght`, `product_description_lenght`, etc.)

2. **Pandas Reading Behavior**: When reading CSV with `keep_default_na=False, na_values=[]`, pandas preserved empty strings as literal `""` instead of converting them to `NaN`

3. **Numeric Conversion Attempt**: The `safe_convert_numeric()` function tried to convert these empty strings to numeric types using `pd.to_numeric()`, which should have worked with `errors='coerce'`

4. **DataFrame to Records Conversion Problem**: When converting the DataFrame back to a list of records, the code had this logic:
   ```python
   # PROBLEMATIC CODE
   if pd.isna(value):
       record[col] = ""  # Converting NaN back to empty string!
   else:
       record[col] = value
   ```

5. **Validation Failure**: During record validation, the script tried to validate empty strings (`""`) against integer/float field types, causing the conversion error

### The Core Problem

The issue was a **round-trip data type inconsistency**:
- Empty CSV cells → Empty strings (`""`) → NaN (via `pd.to_numeric()`) → Empty strings again (`""`) → Validation failure

## Technical Details

### What Should Have Happened
```
Empty CSV cell → "" → NaN → None → Skip validation (valid)
```

### What Actually Happened  
```
Empty CSV cell → "" → NaN → "" → Validation fails trying to convert "" to int/float
```

### Schema Context
The products dataset has these numeric fields that could be empty:
```python
"olist_products_dataset": {
    "field_types": {
        "product_name_lenght": int,      # Could be empty
        "product_description_lenght": int, # Could be empty  
        "product_photos_qty": int,       # Could be empty
        "product_weight_g": float,       # Could be empty
        "product_length_cm": float,      # Could be empty
        "product_height_cm": float,      # Could be empty
        "product_width_cm": float,       # Could be empty
    },
}
```

## Solution Implemented

### 1. Fixed DataFrame to Records Conversion
```python
# BEFORE (problematic)
if pd.isna(value):
    record[col] = ""  # This caused the issue
else:
    record[col] = value

# AFTER (fixed)
if pd.isna(value):
    record[col] = None  # Keep as None, don't convert to empty string
else:
    record[col] = value
```

### 2. Enhanced Validation Logic
```python
# BEFORE
if value is None or value == "":
    continue

# AFTER - More comprehensive empty value handling
if value is None or value == "" or (isinstance(value, str) and value.strip() == ""):
    continue
```

### 3. Better Error Handling
```python
# BEFORE
except ValueError:
    return False, f"Field {field} should be integer, got: {value}"

# AFTER - Handle both ValueError and TypeError
except (ValueError, TypeError):
    return False, f"Field {field} should be integer, got: {value}"
```

## Data Quality Context

This issue highlights a common data engineering challenge:

- **Real-world data is messy**: CSV files often have missing values in numeric columns
- **Multiple representations of "empty"**: `NULL`, `""`, `" "`, `NaN`, `None` all represent missing data
- **Type consistency**: Need to maintain consistent data types throughout the pipeline
- **Validation vs. Coercion**: Balance between strict validation and graceful handling of missing data

## Prevention Strategies

1. **Consistent null handling**: Establish a single representation for missing values throughout the pipeline
2. **Early normalization**: Convert all empty representations to a standard form (like `None`) as early as possible
3. **Schema awareness**: Make field optionality explicit in schemas
4. **Comprehensive testing**: Test with datasets containing various forms of missing data

## Files Affected

- **Primary**: `olist_products_dataset.csv` - Contains many optional numeric fields
- **Potentially affected**: Any dataset with optional numeric fields:
  - `olist_order_items_dataset.csv` (price, freight_value)
  - `olist_geolocation_dataset.csv` (lat, lng coordinates)
  - `olist_order_payments_dataset.csv` (payment_value)

## Testing Recommendations

To prevent similar issues:

1. **Create test datasets** with various empty value representations
2. **Test edge cases**: empty strings, whitespace-only, null values
3. **Validate round-trip consistency**: Data should maintain meaning through all transformations
4. **Schema compliance testing**: Ensure all datasets conform to expected schemas