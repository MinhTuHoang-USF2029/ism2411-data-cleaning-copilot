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