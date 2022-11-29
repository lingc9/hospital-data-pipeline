"""Functions that connect to psql server and load the data

Authors: Carol Ling     <caroll2@andrew.cmu.edu>
#        Xiaochen Sun   <xsun3@andrew.cmu.edu>
#        Xiaonuo Xu     <xiaonuox@andrew.cmu.edu>
"""

import psycopg
import credentials as cred


def connect_to_sql():
    """Creates the connection to PostgreSQL. Prerequisite to this functioning
    properly is the creation of a credentials.py file with the variables
    DB_USER and DB_PASSWORD defined.

    Arguments:
        None.
    Returns:
        conn (connection object): a Pyscopg connection object.
    """
    conn = psycopg.connect(
        host="sculptor.stat.cmu.edu", dbname=cred.DB_USER,
        user=cred.DB_USER, password=cred.DB_PASSWORD
    )
    return conn


def load_hospital_info(cur, data, collect_date):
    """Inserts a row of data into the hospital_info SQL table.

    Arguments:
        cur (cursor object): a Psycopg cursor object.
        data (pd.Dataframe): one row of cleaned data to insert.
        collect_date (str): collection date as a string in the format of
        YYYY-MM-DD.

    Returns:
        Boolean value of True.
    """
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
    """Inserts or update a row of data into the hospital_data SQL table.

    Arguments:
        cur (cursor object): a Psycopg cursor object.
        data (pd.Dataframe): one row of cleaned data to insert/update.

    Returns:
        Boolean value of True.
    """
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
    """Inserts or update a row of data into the hospital_location SQL table.

    Arguments:
        cur (cursor object): a Psycopg cursor object.
        data (pd.Dataframe): one row of cleaned data to insert/update.

    Returns:
        Boolean value of True.
    """
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
    Counts number of hospitals in relations where each hospital_id is unique.

    Arguments:
        cur (cursor object): a Psycopg cursor object.
        tablename (str): name of the SQL table for the query to target.
    """

    if tablename == "hospital_info":
        cur.execute("SELECT COUNT(*) FROM hospital_info")
        return cur.fetchone()[0]
    elif tablename == "hospital_location":
        cur.execute("SELECT COUNT(*) FROM hospital_location")
        return cur.fetchone()[0]
    else:
        raise ValueError("Relation does not exist.")
