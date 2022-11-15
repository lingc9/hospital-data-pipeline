"""
Functions to import and clean csv data

clean_quality_data - converts and cleans hospital general information data

Argumemts:
file_path - directory to where the csv file is stored, in string


clean_hhs_data - converts and cleans hospital general information data

Argumemts:
file_path - directory to where the csv file is stored, in string
"""

import pandas as pd
import numpy as np


def clean_quality_data(file_path):
    df = pd.read_csv(file_path)
    clean_df = pd.DataFrame()

    clean_df["hospital_id"] = df["Facility ID"].astype("str")
    clean_df["name"] = df["Facility Name"].astype("str")
    clean_df["hospital_type"] = df["Hospital Type"].astype("str")
    clean_df["ownership"] = df["Hospital Ownership"].astype("str")
    clean_df["state"] = df["State"].astype("str")
    clean_df["address"] = df["Address"].astype("str")
    clean_df["city"] = df["City"].astype("str")
    clean_df["zip"] = df["ZIP Code"].astype("str")
    clean_df["fips"] = np.nan
    clean_df["latitude"] = np.nan
    clean_df["longitude"] = np.nan
    clean_df["emergency_service"] = df["Emergency Services"]
    # Need to clean and add the ratings later
    # Some data are in the other data frame
    return clean_df


def clean_hhs_data(file_path):
    df = pd.read_csv(file_path)
    clean_df = pd.DataFrame()

    clean_df["hospital_id"] = df["hospital_pk"].astype("str")
    # ...
    # Clean, rename, and add only the lines we need in the table

    return clean_df
