import sys
import os
import pandas as pd

# Add the root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import constants  # noqa: E402

file_name = f"{constants.CSV_PATH}bangalore-station-wise-accidents-2023.csv"
df = pd.read_csv(file_name)
rename_cols = {
    "Zone": "zone",
    "Sub-division": "division",
    "Station": "station",
    "2023 - Fatal Cases": "fatal",
    "2023 - Killed People": "killed",
    "2023 - Non-Fatal": "non_fatal",
    "2023 - Injured People": "injured",
    "2023 - Total Cases": "total",
}

df.rename(columns=rename_cols, inplace=True)
df = df.astype(
    {
        "zone": "string",
        "division": "string",
        "station": "string",
        "fatal": "int16",
        "killed": "int16",
        "non_fatal": "int16",
        "injured": "int16",
        "total": "int16",
    },
)

df.to_parquet(
    f"{constants.PARQUET_PATH}bangalore-station-wise-accidents-2023.parquet",
    index=False,
)

pr_df = pd.read_parquet(
    f"{constants.PARQUET_PATH}bangalore-station-wise-accidents-2023.parquet"
)

# getting sub-divisions totals
zone_na_rows = pr_df["zone"].isna()
division_total = pr_df[zone_na_rows].drop(columns=["zone", "station"])

# getting zone totals
division_na_rows = pr_df["division"].isna()
zone_total = pr_df[division_na_rows].drop(columns=["division", "station"])

# getting pie chart data
pie_chart_data = pr_df[~zone_na_rows & ~division_na_rows].reset_index(drop=True)
print(pie_chart_data)
