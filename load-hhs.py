"""Function and Driver file to load and clean data."""
import sys
import pandas as pd


def convert_to_pd_dataframe(file_path):
    return pd.read_csv(file_path)


weekly_tbl = convert_to_pd_dataframe("./data/hhs_weekly/" + str(sys.argv[1]))
print(weekly_tbl)
