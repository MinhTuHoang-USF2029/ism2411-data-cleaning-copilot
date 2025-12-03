# The code below performs data cleaning on a raw sales data CSV file. The process includes eight steps.
# Some key steps are: Standardizing column names, identifying key columns (product, category, price, quantity, date sold), handling missing or invalid data.
import pandas as pd
# Load raw data
df = pd.read_csv(r'D:\ism2411-data-cleaning-copilot\data\raw\sales_data_raw.csv')
# Standardize column names to make it easier to read and work with
df.columns = (
    df.columns
    .astype(str)
    .str.strip()
    .str.lower()
    .str.replace(r"[^\w]+", "_", regex=True) # Replace non-word characters with underscores
    .str.replace(r"(^_|_$)", "", regex=True) # Remove underscores at start and end of names
)
# Find likely product, category, price, quantity columns by name fragments
def find_column(df, candidates):
    for c in df.columns:
        for s in candidates:
            if s in c:
                return c
    return None
product_col = find_column(df, ["product_name", "product", "name"])
category_col = find_column(df, ["category", "cat"])
price_col = find_column(df, ["price", "unit_price", "unitprice", "sale_price", "amount"])
quantity_col = find_column(df, ["quantity","qty", "units_sold", "units", "count"])
date_sold_col = find_column(df, ["date_sold", "sale_date", "date"])
# Strip leading or trailing whitespace from product names and categories to make them visually clear
for col in (product_col, category_col):
    if col in df.columns and df[col].dtype == object:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
# Title case product and category names
if product_col in df.columns:
        df[product_col] = df[product_col].apply(lambda x: x.title() if isinstance(x, str) else x)
if category_col in df.columns:
        df[category_col] = df[category_col].apply(lambda x: x.title() if isinstance(x, str) else x)
# Coerce price or quantity to numeric (invalid value -> NaN)
if price_col in df.columns:
    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
if quantity_col in df.columns:
    df[quantity_col] = pd.to_numeric(df[quantity_col], errors="coerce")
# Handle missing prices, quantities: drop rows where either is missing 
cols = [price_col, quantity_col]
cols = [c.strip() for c in cols if isinstance(c, str) and c.strip()]
required = [c for c in cols if c in df.columns]
if required:
    df = df.dropna(subset=required)
else:
     print("No invalid price or quantity to clean.")
# Remove rows with clearly invalid values (negative price/quantity or zero price/quantity)
if price_col in df.columns:
    df = df[df[price_col] > 0]
if quantity_col in df.columns:
    df = df[df[quantity_col] > 0]
# Convert date sold to datetime and add rows with invalid dates or missing dates with a specific datetime
def handle_invalid_date(df, fill_datetime=pd.Timestamp("2024-01-07")):
    if date_sold_col in df.columns:
        # Coerce to datetime (invalid -> NaT)
        df[date_sold_col] = pd.to_datetime(df[date_sold_col], errors="coerce", infer_datetime_format=True)
        # Ensure fill_datetime is a pandas Timestamp (not a string)
        if not isinstance(fill_datetime, pd.Timestamp):
            fill_datetime = pd.to_datetime(fill_datetime)
        # Fill missing or invalid dates with the provided datetime (remains datetime dtype)
        df[date_sold_col] = df[date_sold_col].fillna(fill_datetime)
    return df
df = handle_invalid_date(df)
# Final housekeeping and save cleaned data to another csv file
df = df.reset_index(drop=True)
#df.to_csv(r'D:\ism2411-data-cleaning-copilot\data\processed\sales_data_cleaned.csv', index=False)
if __name__ == "__main__":
    raw_path = "data/raw/sales_data_raw.csv"
    cleaned_path = "data/processed/sales_data_clean.csv"

    product_col = find_column(df, ["product_name", "product", "name"])
    category_col = find_column(df, ["category", "cat"])
    price_col = find_column(df, ["price", "unit_price", "unitprice", "sale_price", "amount"])
    quantity_col = find_column(df, ["quantity", "qty", "units_sold", "units", "count"])
    date_sold_col = find_column(df, ["date_sold", "sale_date", "date"])
    df_clean = handle_invalid_date(df)
    df_clean.to_csv(cleaned_path, index=False)
    print("Cleaning complete. First few rows:")
    print(df_clean.head())