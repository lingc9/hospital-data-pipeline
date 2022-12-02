"""
Functions that connect to psql server and perform query to get data

Authors: Carol Ling     <caroll2@andrew.cmu.edu>
#        Xiaochen Sun   <xsun3@andrew.cmu.edu>
#        Xiaonuo Xu     <xiaonuox@andrew.cmu.edu>
"""

import psycopg2
from psycopg2 import sql
import pandas as pd
import credentials as cred


def connect_to_sql2():
    """Creates the connection to PostgreSQL. Prerequisite to this functioning
    properly is the creation of a credentials.py file with the variables
    DB_USER and DB_PASSWORD defined.

    Arguments:
        None.
    Returns:
        conn (connection object): a Pyscopg connection object.
    """
    conn = psycopg2.connect(
        host="sculptor.stat.cmu.edu", dbname=cred.DB_USER,
        user=cred.DB_USER, password=cred.DB_PASSWORD
    )
    return conn


def get_distinct_collection_date(cur, tablename):
    """Gets all collection dates in a relation

    Arguments:
        cur (cursor object): a Psycopg cursor object.
        tablename (string): name of any of the three relations:
                            hospital_info, hospital_data, hospital_location

    Returns:
        A list containing all collection dates, sorted by chronological order
    """

    try:
        cur.execute(sql.SQL("SELECT DISTINCT collection_date FROM {} "
                            "ORDER BY collection_date ASC")
                    .format(sql.Identifier(tablename)))
    except psycopg2.errors.UndefinedTable:
        raise Exception("Relation doesn not exist in the server.")
    lists = [item for sublist in cur.fetchall() for item in sublist]
    return lists


def get_previous_weeks(cur, tablename, collect_date):
    """Gets all weeks previous (including current) collection date if any

    Arguments:
        cur (cursor object): a Psycopg cursor object.
        tablename (string): name of any of the three relations:
                            hospital_info, hospital_data, hospital_location
        collect_date (datetime.date): collection date of interest

    Returns:
        A list containing all previous (including current) collection date
        If no date exists, return False
        If data set does not contain data from that date, return False
    """

    lists = get_distinct_collection_date(cur, tablename)

    if len(lists) == 0:
        return False
    else:
        try:
            index = lists.index(collect_date)
            return lists[0:index+1]
        except ValueError:
            return False


def get_records_number(cur, tablename, collect_date):
    """Number of records loaded in the most recent and previous weeks

    Arguments:
        cur (cursor object): a Psycopg cursor object.
        tablename (string): name of any of the three relations:
                            hospital_info, hospital_data, hospital_location
        collect_date (datetime.date): collection date of interest

    Returns:
        A dictionary using datetime as key and number of records as value
        If the collection date is not in the table, return False
    """

    lists = get_previous_weeks(cur, tablename, collect_date)
    record_number = []

    if lists:
        for i in range(len(lists)):
            try:
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {} WHERE "
                                    "collection_date = CAST(%s AS DATE)")
                            .format(sql.Identifier(tablename)),
                            (lists[i],))
            except psycopg2.errors.UndefinedTable:
                raise Exception("Relation doesn not exist in the server.")
            record_number.append(cur.fetchone()[0])
            print(str(tablename) + " contains "
                  + str(record_number[i]) + " records from "
                  + str(lists[i]))
        return dict(zip(lists, record_number))
    else:
        return False


def get_beds_detail(conn, collect_date):
    """Get the number of adult and pediatric beds available this week,
       the number used, and the number used by patients with COVID,
       compared to the 4 most recent weeks

    Arguments:
        conn (connection object): a Psycopg connection object.
        collect_date (datetime.date): collection date of interest

    Returns:
        A table containing all information above for the 4 most recent weeks
        If no data on that collection_date exists, return False
    """

    cur = conn.cursor()
    lists = get_previous_weeks(cur, "hospital_data", collect_date)
    colnames = ["avalible_adult_beds", "avalible_pediatric_beds",
                "occupied_adult_beds", "occupied_pediatric_beds",
                "available_icu_beds", "occupied_icu_beds", "covid_beds_use"]
    cur.close()

    if lists:
        # Only take the 4 most recent weeks and the current one
        if len(lists) > 5:
            lists = lists[-5:]
        for i in range(len(lists)):
            try:
                query = sql.SQL("SELECT SUM(NULLIF({}, 'NaN')), "
                                "collection_date FROM {} "
                                "GROUP BY collection_date") \
                        .format(sql.SQL(", 'NaN')), SUM(NULLIF(")
                                .join(map(sql.Identifier, colnames)),
                                sql.Identifier("hospital_data"))
                df = pd.read_sql_query(query, conn)
            except psycopg2.errors.UndefinedTable:
                raise Exception("Relation doesn not exist in the server.")
        colnames.append("collection_date")
        df.set_axis(colnames, axis=1, inplace=True)
        return df
    else:
        return False
