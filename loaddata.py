"""
Functions that connect to psql server and load the data


connect_to_sql - creats a connection to postgresql

load_hospital_data - insert a row of data into hospital_data

load_hospital_info - insert or update a row of data into hospital_info

load_hospital_location - insert or update a row of data into hospital_location

update_hospital_info - update a hospital's inforamtion in hospital_info

update_hospital_location - update a hospital's location in hospital_location

insert_hospital_info - insert a new hospital to hospital_info

insert_hospital_location - insert a new hospital to hospital_location

check_hospital_info - check if a hospital exist in hospital_info

check_hospital_location - check if a hospital exist in hospital_location

Arguments:

dict_info - dictionary containing all hospital id from hospital_info

dict_location - dictionary containing all hospital id from hospital_location

conn - psycopg connect to postgresql server

data - a row of pd data frame containing the data to be inserted

collect_date - the date when the quality file is collected
"""

import psycopg
import credentials as cred


def connect_to_sql():
    conn = psycopg.connect(
        host="sculptor.stat.cmu.edu", dbname=cred.DB_USER,
        user=cred.DB_USER, password=cred.DB_PASSWORD
    )
    return conn


def load_hospital_info(cur, data, collect_date):
    try:
        cur.execute("INSERT INTO hospital_info (hospital_id, name, "
                    "hospital_type, ownership, collection_date, state, "
                    "address, city, zip, emergency_service, quality_rating)"
                    "VALUES (%(hospital_id)s, %(name)s, %(hospital_type)s, "
                    "%(ownership)s, %(collection_date)s, %(state)s, "
                    "%(address)s, %(city)s, %(zip)s, %(emergency_service)s, "
                    "%(quality_rating)s) "
                    "ON CONFLICT (hospital_id) DO UPDATE SET "
                    "name = EXCLUDED.name,"
                    "hospital_type = EXCLUDED.hospital_type,"
                    "ownership = EXCLUDED.ownership,"
                    "collection_date = EXCLUDED.collection_date,"
                    "state = EXCLUDED.state,"
                    "address = EXCLUDED.address,"
                    "city = EXCLUDED.city,"
                    "zip = EXCLUDED.zip,"
                    "emergency_service = EXCLUDED.emergency_service,"
                    "quality_rating = EXCLUDED.quality_rating",
                    {
                     "hospital_id": data["hospital_id"],
                     "name": data["name"],
                     "hospital_type": data["hospital_type"],
                     "ownership": data["ownership"],
                     "collection_date": collect_date,
                     "state": data["state"],
                     "address": data["address"],
                     "city": data["city"],
                     "zip": data["zip"],
                     "emergency_service": data["emergency_service"],
                     "quality_rating": data["quality_rating"],
                    }
                    )
    except psycopg.errors.UndefinedTable:
        raise Exception("Relation doesn not exist in the server.")
    return True


def load_hospital_data(cur, data):
    try:
        cur.execute("INSERT INTO hospital_data (hospital_id, collection_date, "
                    "avalible_adult_beds, avalible_pediatric_beds, "
                    "occupied_adult_beds, occupied_pediatric_beds, "
                    "available_ICU_beds, occupied_ICU_beds, "
                    "COVID_beds_use, COVID_ICU_use)"
                    "VALUES (%(hospital_id)s, %(collection_date)s, "
                    "%(avalible_adult_beds)s, %(avalible_pediatric_beds)s, "
                    "%(occupied_adult_beds)s, %(occupied_pediatric_beds)s, "
                    "%(available_ICU_beds)s, %(occupied_ICU_beds)s, "
                    "%(COVID_beds_use)s, %(COVID_ICU_use)s) "
                    "ON CONFLICT DO NOTHING",
                    {
                     "hospital_id": data["hospital_id"],
                     "collection_date": data["collection_date"],
                     "avalible_adult_beds": data["avalible_adult_beds"],
                     "avalible_pediatric_beds":
                     data["avalible_pediatric_beds"],
                     "occupied_adult_beds": data["occupied_adult_beds"],
                     "occupied_pediatric_beds":
                     data["occupied_pediatric_beds"],
                     "available_ICU_beds": data["available_ICU_beds"],
                     "occupied_ICU_beds": data["occupied_ICU_beds"],
                     "COVID_beds_use": data["COVID_beds_use"],
                     "COVID_ICU_use": data["COVID_ICU_use"]
                    }
                    )
    except psycopg.errors.UndefinedTable:
        raise Exception("Relation doesn not exist in the server.")
    return True


def load_hospital_location(cur, data):
    try:
        cur.execute("INSERT INTO hospital_location (hospital_id, "
                    "collection_date, fips, latitude, longitude)"
                    "VALUES (%(hospital_id)s, %(collection_date)s, "
                    " %(fips)s, %(latitude)s, %(longitude)s)"
                    "ON CONFLICT (hospital_id) DO UPDATE SET "
                    "collection_date = EXCLUDED.collection_date,"
                    "fips = EXCLUDED.fips,"
                    "latitude = EXCLUDED.latitude,"
                    "longitude = EXCLUDED.longitude",
                    {
                     "hospital_id": data["hospital_id"],
                     "collection_date": data["collection_date"],
                     "fips": data["fips"],
                     "latitude": data["latitude"],
                     "longitude": data["longitude"]
                    }
                    )
    except psycopg.errors.UndefinedTable:
        raise Exception("Relation doesn not exist in the server.")
    return True


def count_hospitals(cur, tablename):
    """
    Counts number of hospitals in relations where each hospital_id is unique

    Argument:
    cur - connection object from the server
    tablename - name of the relation to do query using
    """

    if tablename == "hospital_info":
        cur.execute("SELECT COUNT(*) FROM hospital_info")
        return cur.fetchone()[0]
    elif tablename == "hospital_location":
        cur.execute("SELECT COUNT(*) FROM hospital_location")
        return cur.fetchone()[0]
    else:
        raise ValueError("Relation does not exist.")
