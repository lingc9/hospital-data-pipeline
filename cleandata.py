"""
Functions to import and clean csv data


format_geocode - turns geocoded hospital address into longitude and latitude

Arguments:
code - geocoded address stored as string


clean_quality_data - converts and cleans hospital general information data

Argumemts:
file_path - directory to where the csv file is stored, in string


clean_hhs_data - converts and cleans hospital general information data

Argumemts:
file_path - directory to where the csv file is stored, in string


format_geocode - changes geocode from PONT() to latitude and longitude

Arguments:
geocode - String like this "POINT (longitude, latitude)"
"""

import pandas as pd


def format_geocode(geocode):
    ans = geocode.strip("POINT (")
    ans = ans.strip(")")
    list = ans.split(" ")

    long = float(list[0])
    lat = float(list[1])
    return long, lat


def clean_quality_data(file_path):
    df = pd.read_csv(file_path)
    clean_df = pd.DataFrame()

    missing = df["Hospital overall rating"] == "Not Available"
    df["Hospital overall rating"][missing] = None

    clean_df["hospital_id"] = df["Facility ID"].astype("str")
    clean_df["name"] = df["Facility Name"].astype("str")
    clean_df["hospital_type"] = df["Hospital Type"].astype("str")
    clean_df["ownership"] = df["Hospital Ownership"].astype("str")
    clean_df["state"] = df["State"].astype("str")
    clean_df["address"] = df["Address"].astype("str")
    clean_df["city"] = df["City"].astype("str")
    clean_df["zip"] = df["ZIP Code"].astype("str")
    clean_df["emergency_service"] = df["Emergency Services"]
    clean_df["quality_rating"] = df["Hospital overall rating"]
    # Need to clean and add the ratings later
    return clean_df


def clean_hhs_data(file_path):
    df = pd.read_csv(file_path)
    clean_df = pd.DataFrame(file_path)

    df = df.fillna (value = 'None')

    missing_value = df["all_adult_hospital_beds_7_day_avg"] < 0
    df["all_adult_hospital_beds_7_day_avg"][missing_value] = None

    missing_value = df["all_pediatric_inpatient_beds_7_day_avg"] < 0
    df["all_pediatric_inpatient_beds_7_day_avg"][missing_value] = None

    missing_value = df["all_adult_hospital_inpatient_bed_occupied_7_day_coverage"] < 0
    df["all_adult_hospital_inpatient_bed_occupied_7_day_coverage"][missing_value] = None

    missing_value = df["total_icu_beds_7_day_avg"] < 0
    df["total_icu_beds_7_day_avg"][missing_value] = None

    missing_value = df["icu_beds_used_7_day_avg"] < 0
    df["icu_beds_used_7_day_avg"][missing_value] = None

    missing_value = df["inpatient_beds_used_covid_7_day_avg"] < 0
    df["inpatient_beds_used_covid_7_day_avg"][missing_value] = None

    missing_value = df["staffed_icu_adult_patients_confirmed_covid_7_day_coverage"] < 0
    df["staffed_icu_adult_patients_confirmed_covid_7_day_coverage"][missing_value] = None

    # Clean the data here (remove NA. -999, etc.)
    
    long, lat = format_geocode(file_path)

    clean_df["hospital_id"] = df["hospital_pk"].astype("str")
    clean_df["collection_date"] = pd.to_datetime(df["collection_week"],
                                                 format="%Y-%m-%d")
    clean_df["avalible_adult_beds"] = df["all_adult_hospital_beds_7_day_avg"]
    clean_df["avalible_pediatric_beds"] = df[
        "all_pediatric_inpatient_beds_7_day_avg"]
    clean_df["occupied_adult_beds"] = df[
        "all_adult_hospital_inpatient_bed_occupied_7_day_coverage"]
    clean_df["occupied_pediatric_beds"] = df[
        "all_pediatric_inpatient_bed_occupied_7_day_avg"]
    clean_df["available_ICU_beds"] = df["total_icu_beds_7_day_avg"]
    clean_df["occupied_ICU_beds"] = df["icu_beds_used_7_day_avg"]
    clean_df["COVID_beds_use"] = df["inpatient_beds_used_covid_7_day_avg"]
    clean_df["COVID_ICU_use"] = df[
        "staffed_icu_adult_patients_confirmed_covid_7_day_coverage"]
    clean_df["fips"] = df["fips_code"]
    clean_df["address"] = df["geocoded_hospital_address"]
    clean_df["latitude"] = lat
    clean_df["longitude"] = long

    return clean_df
