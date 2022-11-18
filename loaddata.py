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
import pandas as pd
import numpy as np
import credentials as cred


def connect_to_sql():
    conn = psycopg.connect(
        host="sculptor.stat.cmu.edu", dbname=cred.DB_USER,
        user=cred.DB_USER, password=cred.DB_PASSWORD
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


def load_hospital_data(conn, data):
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


def insert_hospital_location(conn, data):
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


def check_table_exists(conn, tablename):
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if cur.fetchone()[0] == 1:
        cur.close()
        return True

    cur.close()
    return False


def create_dict(conn, table):
    """Create a dictionary by hospital_id as the key from pre-existing
    hopital_info table with remaining columns as dictionary values."""

    d = pd.read_sql_query("SELECT * FROM %s" % table, conn)

    if d.empty:
        sql_dict = {}
    else:
        sql_dict = d.set_index("hospital_id").to_dict('index')

    return sql_dict

    # Implementation through the psycopg library
    # cur = conn.cursor()

    # cur.execute("SELECT * FROM hospital_info ")

    # rows = cur.fetchall()

    # d = {}
    # for row in rows:
    #     key = row[0]
    #     value = row[1:]
    #     try:
    #         d[key].append(value)
    #     except KeyError:
    #         d[key] = [value]


def update_hospital_info(conn, data, collect_date):
    cur = conn.cursor()
    # exist_id = check_hospital_id(conn, table, data)
    # print()
    # to_update = data[data["hospital_id"]]

    cur.execute(
        "UPDATE hospital_info AS f "
        "SET hospital_id = %(hospital_id)s,"
        "name = %(name)s,"
        "hospital_type = %(hospital_type)s,"
        "ownership = %(ownership)s,"
        "collection_date = %(collect_date)s,"
        "state = %(state)s,"
        "address = %(address)s,"
        "city = %(city)s,"
        "zip = %(zip)s,"
        "emergency_service = %(emergency_service)s,"
        "quality_rating = %(quality_rating)s"
        "WHERE f.hospital_id = %(hospital_id)s",
        {"hospital_id": data["hospital_id"],
         "name": data["name"],
         "hospital_type": data["hospital_type"],
         "ownership": data["ownership"],
         "collect_date": collect_date,
         "state": data["state"],
         "address": data["address"],
         "city": data["city"],
         "zip": data["zip"],
         "emergency_service": data["emergency_service"],
         "quality_rating": data["quality_rating"],
         })


def update_hospital_location(conn, data):
    cur = conn.cursor()

    cur.execute(
        "UPDATE hospital_location AS f "
        "SET hospital_id = %(hospital_id)s,"
        "collection_date = %(collect_date)s,"
        "fips = %(state)s,"
        "latitude = %(address)s,"
        "longitutde = %(city)s,"
        "zip = %(zip)s,"
        "emergency_service = %(emergency_service)s,"
        "quality_rating = %(quality_rating)s"
        "WHERE f.hospital_id = %(hospital_id)s",
        {"hospital_id": data["hospital_id"],
         "collection_date": data["collection_date"],
         "fips": data["fips"],
         "latitude": data["latitude"],
         "longitude": data["longitude"]
         })


def check_hospital_id(conn, table, data):
    """Checks incoming data to see if new hospitals have been added and returns
    an array of the pre-existing hospital ids."""
    sql_dict = create_dict(conn, table)

    ids = np.array([data["hospital_id"]])
    key_array = np.array(list(sql_dict.keys()))
    exists = np.intersect1d(ids, key_array)

    return exists


def check_hospital_location(conn, data):
    # dict_location
    # Unnecessary?
    return False


def load_hospital_info(conn, table, data, collect_date):
    """Helper function to load the hospital data
    Parameters:
        data: a pd dataframe of information we have read out of the .csv file
        collect_date: the collection date"""
    if check_table_exists(conn, table) is False:
        raise ValueError("Table %s does not exist" % table)

    existing_hosp = check_hospital_id(conn, table, data)

    if len(existing_hosp) == 0:
        insert_hospital_info(conn, data, collect_date)
    else:  # When array of existing hospital is not empty, update
        update_hospital_info(conn, data, collect_date)

    return True


def load_hospital_location(conn, table, data):
    if check_table_exists(conn, table) is False:
        raise ValueError("Table %s does not exist" % table)

    existing_hosp = check_hospital_id(conn, table, data)

    if len(existing_hosp) == 0:
        insert_hospital_location(conn, data)
    else:  # When array of existing hospital is not empty, update
        update_hospital_location(conn, data)

    return True