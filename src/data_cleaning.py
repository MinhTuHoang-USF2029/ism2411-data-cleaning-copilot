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
quantity_col = find_column(df, ["quantity", "qty", "units_sold", "units", "count"])
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
# Coerce price/quantity to numeric (invalid value -> NaN)
if price_col in df.columns:
    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
if quantity_col in df.columns:
    df[quantity_col] = pd.to_numeric(df[quantity_col], errors="coerce")