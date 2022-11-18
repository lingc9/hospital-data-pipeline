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

import credentials


def connect_to_sql():
    conn = psycopg.connect(
        host="sculptor.stat.cmu.edu", dbname=credentials.DB_USER,
        user=credentials.DB_USER, password=credentials.DB_PASSWORD
    )
    return conn


def insert_hospital_info(conn, data, collect_date):
    cur = conn.cursor()

    cur.execute("INSERT INTO hospital_info (hospital_id, name, hospital_type, "
                "ownership, collection_date, state, address, city, zip, "
                "emergency_service, quality_rating)"
                "VALUES (%(hospital_id)s, %(name)s, %(hospital_type)s, "
                "%(ownership)s, %(collection_date)s, %(state)s, "
                "%(address)s, %(city)s, %(zip)s, %(emergency_service)s, "
                "%(quality_rating)s)",
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

    return True


def load_hospital_data(conn, data, collect_date):
    cur = conn.cursor()

    cur.execute("INSERT INTO hospital_data (hospital_id, collection_date, "
                "avalible_adult_beds, avalible_pediatric_beds, "
                "occupied_adult_beds, occupied_pediatric_beds, "
                "available_ICU_beds, occupied_ICU_beds, "
                "COVID_beds_use, COVID_ICU_use)"
                "VALUES (%(hospital_id)s, %(collection_date)s, "
                "%(avalible_adult_beds)s, %(avalible_pediatric_beds)s, "
                "%(occupied_adult_beds)s, %(occupied_pediatric_beds)s, "
                "%(available_ICU_beds)s, %(occupied_ICU_beds)s, "
                "%(COVID_beds_use)s, %(COVID_ICU_use)s)",
                {
                 "hospital_id": data["hospital_id"],
                 "collection_date": data["collection_date"],
                 "avalible_adult_beds": data["avalible_adult_beds"],
                 "avalible_pediatric_beds": data["avalible_pediatric_beds"],
                 "occupied_adult_beds": data["occupied_adult_beds"],
                 "occupied_pediatric_beds": data["occupied_pediatric_beds"],
                 "available_ICU_beds": data["available_ICU_beds"],
                 "occupied_ICU_beds": data["occupied_ICU_beds"],
                 "COVID_beds_use": data["COVID_beds_use"],
                 "COVID_ICU_use": data["COVID_ICU_use"]
                }
                )

    return True


def insert_hospital_location(conn, data, collect_date):
    cur = conn.cursor()

    cur.execute("INSERT INTO hospital_location (hospital_id, "
                "collection_date, fips, latitude, longitude)"
                "VALUES (%(hospital_id)s, %(collection_date)s, "
                " %(fips)s, %(latitude)s, %(longitude)s)",
                {
                 "hospital_id": data["hospital_id"],
                 "collection_date": data["collection_date"],
                 "fips": data["fips"],
                 "latitude": data["latitude"],
                 "longitude": data["longitude"]
                }
                )

    return True


def update_hospital_info(conn, data, collect_date):
    pass


def update_hospital_location(conn, data, collect_date):
    pass


def check_hospital_info(conn, data):
    # Should take in a dictionary of hospitals as argument
    # dict_info
    return False


def check_hospital_location(conn, data):
    # dict_location
    return False


def load_hospital_info(conn, data, collect_date):
    if check_hospital_info(conn, data):  # Check if the hospital exist
        update_hospital_info(conn, data, collect_date)

    else:
        insert_hospital_info(conn, data, collect_date)

    return True


def load_hospital_location(conn, data, collect_date):
    if check_hospital_location(conn, data):  # Check if the hospital exist
        update_hospital_location(conn, data, collect_date)

    else:
        insert_hospital_location(conn, data, collect_date)

    return True
