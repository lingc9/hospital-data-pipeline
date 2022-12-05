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
        raise Exception("Relation does not exist in the server.")
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


def get_records_number(conn, tablename, collect_date):
    """Number of records loaded in the most recent and previous weeks

    Arguments:
        conn (connection object): a Psycopg connection object.
        tablename (string): name of any of the three relations:
                            hospital_info, hospital_data, hospital_location
        collect_date (datetime.date): collection date of interest

    Returns:
        A dictionary using datetime as key and number of records as value
        If the collection date is not in the table, return False
    """

    cur = conn.cursor()
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
                raise Exception("Relation does not exist in the server.")
            record_number.append(cur.fetchone()[0])
        cur.close()
        return dict(zip(lists, record_number))
    else:
        return False


def get_beds_detail(conn, collect_date, recent):
    """Get the number of adult and pediatric beds available this week,
       the number used, and the number used by patients with COVID

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
                "available_icu_beds", "occupied_adult_beds",
                "occupied_pediatric_beds", "occupied_icu_beds",
                "covid_beds_use", "covid_icu_use"]
    cur.close()

    if lists:
        try:
            query = sql.SQL("SELECT SUM(NULLIF({}, 'NaN')), "
                            "collection_date FROM {} "
                            "GROUP BY collection_date "
                            "ORDER BY collection_date DESC") \
                    .format(sql.SQL(", 'NaN')), SUM(NULLIF(")
                            .join(map(sql.Identifier, colnames)),
                            sql.Identifier("hospital_data"))
            df = pd.read_sql_query(query, conn)
        except psycopg2.errors.UndefinedTable:
            raise Exception("Relation does not exist in the server.")

        # Subset the 4 most recent weeks if recent restriction is in place
        if len(lists) > 5 and recent:
            index = df.index[df['collection_date'] == collect_date].tolist()
            df = df.iloc[index[0]:index[0]+4]

        # Change name of the data frame
        colnames.append("collection_date")
        df.set_axis(colnames, axis=1, inplace=True)
        df.iloc[:, :-1] = df.iloc[:, :-1].astype(int)

        # Calculate non_covid bed use
        df["non_covid_beds_use"] = \
            df["occupied_adult_beds"] + df["occupied_pediatric_beds"]

        return df
    else:
        return False


def get_beds_sum_by(conn, collect_date, property):
    """Get the number of beds in use by specified property in given week

    Arguments:
        conn (connection object): a Psycopg connection object.
        collect_date (datetime.date): collection date of interest
        property (string): any variable in hospital_info relation
                           quality_rating / state / ownership

    Returns:
        A table containing sum and utlization rate of beds grouped by property
        If no data on that collection_date exists, return False
    """

    cur = conn.cursor()
    lists_data = get_previous_weeks(cur, "hospital_data", collect_date)
    lists_info = get_distinct_collection_date(cur, "hospital_info")
    colnames = ["avalible_adult_beds", "avalible_pediatric_beds",
                "available_icu_beds", "occupied_adult_beds",
                "occupied_pediatric_beds", "occupied_icu_beds",
                "covid_beds_use"]
    cur.close()

    if lists_data and lists_info:
        try:
            query = sql.SQL("SELECT SUM(NULLIF({}, 'NaN')), {prop} "
                            "FROM (SELECT hospital_id, {} FROM hospital_data "
                            "WHERE collection_date = CAST('{}' AS DATE)) AS d "
                            "INNER JOIN (SELECT hospital_id, {prop} "
                            "FROM hospital_info) AS i ON "
                            "d.hospital_id = i.hospital_id "
                            "GROUP BY {prop}") \
                    .format(sql.SQL(", 'NaN')), SUM(NULLIF(")
                            .join(map(sql.Identifier, colnames)),
                            sql.SQL(', ')
                            .join(map(sql.Identifier, colnames)),
                            sql.SQL(str(collect_date)),
                            prop=sql.SQL(property))
            df = pd.read_sql_query(query, conn)
        except psycopg2.errors.UndefinedTable:
            raise Exception("Relation does not exist in the server.")

        # Change name of the data frame
        colnames.append(property)
        df.set_axis(colnames, axis=1, inplace=True)

        # Calculate utilization rate
        df["adult_utilization"] = \
            (df["occupied_adult_beds"]/df["avalible_adult_beds"])\
            .round(decimals=2)
        df["pediatric_utilization"] = \
            (df["occupied_pediatric_beds"]/df["avalible_pediatric_beds"])\
            .round(decimals=2)
        df["icu_utilization"] = \
            (df["occupied_icu_beds"]/df["available_icu_beds"])\
            .round(decimals=2)

        return df
    else:
        return False


def get_hospital(conn, collect_date):
    """Get the number of covid beds in use by hospital_id

    Arguments:
        conn (connection object): a Psycopg connection object.
        collect_date (datetime.date): collection date of interest

    Returns:
        A table containing sum of covid beds in use grouped by hospital_id
        If no data on that collection_date exists, return False
    """

    colnames = ["name", "city", "state", "zip", "address"]

    try:
        query = sql.SQL("SELECT i.hospital_id, covid_bed_case, covid_icu_case,"
                        " {} FROM (SELECT hospital_id, "
                        "SUM(NULLIF(COVID_beds_use, 'NaN')) AS covid_bed_case,"
                        " SUM(NULLIF(COVID_ICU_use, 'Nan')) AS "
                        "covid_icu_case FROM hospital_data WHERE "
                        "collection_date = CAST('{}' AS DATE) "
                        "GROUP BY hospital_id) AS d "
                        "INNER JOIN (SELECT hospital_id, {} FROM hospital_info"
                        ") AS i ON d.hospital_id = i.hospital_id") \
                .format(sql.SQL(", ")
                        .join(map(sql.Identifier, colnames)),
                        sql.SQL(str(collect_date)),
                        sql.SQL(", ")
                        .join(map(sql.Identifier, colnames)))
        df = pd.read_sql_query(query, conn)
    except psycopg2.errors.UndefinedTable:
        raise Exception("Relation does not exist in the server.")
    return df


def get_covid_total():
    pass


def get_covid_change(conn, collect_date, nshow, property):
    """Rank the states/hospitals by the number of cases since the last week

    Arguments:
        conn (connection object): a Psycopg connection object.
        collect_date (datetime.date): collection date of interest
        nshow (int): number of observations to display

    Returns:
        A table containing change in covid case and current case, sorted
        If no data on that collection_date exists, return False
    """

    cur = conn.cursor()
    lists = get_previous_weeks(cur, "hospital_data", collect_date)
    cur.close()

    if lists:
        if len(lists) > 1 and property == "state":
            index = lists.index(collect_date)
            lastweek = lists[index-1]
            new_data = get_beds_sum_by(conn, collect_date,
                                       property).iloc[:, 5:8]
            new_data["covid_cases"] = new_data.iloc[:, 0] + new_data.iloc[:, 1]
            new_data["covid_cases"] = \
                new_data["covid_cases"].round(0).astype('Int64')
            new_data = new_data[["covid_cases", "state"]]
            old_data = get_beds_sum_by(conn, lastweek,
                                       property).iloc[:, 5:8]
            old_data["covid_cases"] = old_data.iloc[:, 0] + old_data.iloc[:, 1]
            old_data["covid_cases"] = \
                old_data["covid_cases"].round(0).astype('Int64')
            old_data = old_data[["covid_cases", "state"]]

            new_data.columns = ["covid_"+str(collect_date), property]
            old_data.columns = ["covid_"+str(lastweek), property]

            # Calculate the change in covid since last week
            change_data = new_data.join(old_data.set_index(property),
                                        on=property)
            change_data = change_data.dropna()
            change_data["change_covid_bed_use"] = \
                change_data.iloc[:, 0] - change_data.iloc[:, 2]

            # Sort by the change
            change_data = change_data.iloc[(
                -change_data["change_covid_bed_use"].abs()).argsort()]
            change_data = change_data.iloc[:nshow]
            return change_data.reindex(sorted(change_data.columns), axis=1)

        elif len(lists) > 1 and property == "hospital_id":
            index = lists.index(collect_date)
            lastweek = lists[index-1]
            new_data = get_hospital(conn, collect_date)
            new_data.insert(loc=1, column="covid_cases",
                            value=new_data.iloc[:, 1] + new_data.iloc[:, 2])
            new_data["covid_cases"] = \
                new_data["covid_cases"].round(0).astype('Int64')
            new_data = new_data.drop(new_data.iloc[:, 2:4], axis=1)
            old_data = get_hospital(conn, lastweek).iloc[:, 0:3]
            old_data.insert(loc=1, column="covid_cases",
                            value=old_data.iloc[:, 1] + old_data.iloc[:, 2])
            old_data["covid_cases"] = \
                old_data["covid_cases"].round(0).astype('Int64')
            old_data = old_data.drop(old_data.iloc[:, 2:4], axis=1)
            old_data.columns = [property, "covid_"+str(lastweek)]

            # Calculate the change in covid since last week
            change_data = new_data.join(old_data.set_index(property),
                                        on=property)
            change_data = change_data.dropna()
            change_data["change_covid_bed_use"] = \
                change_data.iloc[:, 1] - change_data.iloc[:, 7]

            # Sort by the change
            change_data = change_data.iloc[(
                -change_data["change_covid_bed_use"].abs()).argsort()]
            change_data = change_data.iloc[:nshow]
            return change_data  # .reindex(sorted(change_data.columns), axis=1)
        else:
            return False
    else:
        return False
