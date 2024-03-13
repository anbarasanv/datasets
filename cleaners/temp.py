import pandas as pd
import pyarrow.parquet as pq


# GitHub URL of the Parquet file
github_url = "https://github.com/anbarasanv/datasets/raw/main/parquet/bangalore-station-wise-accidents-2023.parquet"

df = pd.read_parquet(github_url)

print(df)
# df = pq.read_table(parquet_file).to_pandas()

# Now you can work with the 'df' DataFrame which contains the data from the Parquet file
