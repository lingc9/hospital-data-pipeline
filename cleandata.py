"""Functions to import and clean the raw csv data from a relative or absolute
file path.

Authors: Carol Ling     <caroll2@andrew.cmu.edu>
#        Xiaochen Sun   <xsun3@andrew.cmu.edu>
#        Xiaonuo Xu     <xiaonuox@andrew.cmu.edu>
"""

import pandas as pd


def clean_quality_data(file_path):
    """Converts and cleans hospital data from Centers for Medicare and Medicaid
    Services (CMS).

    Arguments:
        file_path (str): file path to the directory to where the raw csv is
        stored.

    Returns:
        clean_df (pd.Dataframe): a clean pd.Dataframe object.
    """
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
    return clean_df


def clean_hhs_data(file_path):
    """
    Converts and cleans general hospital data from the US Department of Health
    and Human Services (HHS).

    Arguments:
        file_path (str): file path to the directory to where the raw csv is
        stored.

    Returns:
        clean_df (pd.Dataframe): a clean pd.Dataframe object.
    """
    df = pd.read_csv(file_path)
    clean_df = pd.DataFrame()

    df = df.replace("NA", None)
    df = df.replace("NULL", None)
    df.loc[df.loc[:, "all_adult_hospital_beds_7_day_avg" ] < 0,
           "all_adult_hospital_beds_7_day_avg"] = None
    
    df.loc[df.loc[:, "all_pediatric_inpatient_beds_7_day_avg" ] < 0,
           "all_pediatric_inpatient_beds_7_day_avg"] = None
    
    df.loc[df.loc[:, "all_adult_hospital_inpatient_bed_occupied_7_day_coverage" ] < 0,
           "all_adult_hospital_inpatient_bed_occupied_7_day_coverage"] = None

    df.loc[df.loc[:, "total_icu_beds_7_day_avg" ] < 0,
           "total_icu_beds_7_day_avg"] = None
    
    df.loc[df.loc[:, "icu_beds_used_7_day_avg" ] < 0,
           "icu_beds_used_7_day_avg"] = None

    df.loc[df.loc[:, "inpatient_beds_used_covid_7_day_avg" ] < 0,
           "inpatient_beds_used_covid_7_day_avg"] = None
    
    df.loc[df.loc[:, "staffed_icu_adult_patients_confirmed_covid_7_day_coverage" ] < 0,
           "staffed_icu_adult_patients_confirmed_covid_7_day_coverage"] = None
    
    df.loc[df.loc[:, "all_pediatric_inpatient_bed_occupied_7_day_avg" ] < 0,
           "all_pediatric_inpatient_bed_occupied_7_day_avg"] = None

    clean_df["hospital_id"] = df["hospital_pk"].astype("str")

    clean_df["collection_date"] = df["collection_week"].apply(pd.to_datetime)

    clean_df["avalible_adult_beds"] = df["all_adult_hospital_beds_7_day_avg"
                                         ].astype("float")

    clean_df["avalible_pediatric_beds"] = df[
        "all_pediatric_inpatient_beds_7_day_avg"].astype("float")

    clean_df["occupied_adult_beds"] = df[
             "all_adult_hospital_inpatient_bed_occupied_7_day_coverage"
            ].astype("float")

    clean_df["occupied_pediatric_beds"] = df[
        "all_pediatric_inpatient_bed_occupied_7_day_avg"].astype("float")

    clean_df["available_ICU_beds"] = df[
             "total_icu_beds_7_day_avg"
            ].astype("float")

    clean_df["occupied_ICU_beds"] = df[
             "icu_beds_used_7_day_avg"
            ].astype("float")

    clean_df["COVID_beds_use"] = df[
             "inpatient_beds_used_covid_7_day_avg"
            ].astype("float")

    clean_df["COVID_ICU_use"] = df[
             "staffed_icu_adult_patients_confirmed_covid_7_day_coverage"
            ].astype("float")

    clean_df["fips"] = df["fips_code"].astype("float")
    clean_df["address"] = df["address"].astype("str")
    gcode = df.geocoded_hospital_address
    long = gcode.apply(lambda x:
                       str(x).strip("POINT ( )").split(" ")[0]).astype("float")
    lat = gcode.apply(lambda x:
                      str(x).strip("POINT ( )").split(" ")[-1]).astype("float")

    clean_df["latitude"] = lat
    clean_df["longitude"] = long

    return clean_df
