"""
Functions to import and clean csv data

convert_to_pd_dataframe - converts csv file into pd data frame

Arguments:
file_path - directory to where the csv file is stored, in string
"""

import pandas as pd


def convert_to_pd_dataframe(file_path):
    return pd.read_csv(file_path)
