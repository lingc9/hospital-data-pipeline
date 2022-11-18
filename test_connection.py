"""
This is a test function for postgresql server connection

Delete this file from the project once done
"""

from loaddata import connect_to_sql
import pandas as pd


def test_connection(text):
    conn = connect_to_sql()
    d = pd.read_sql_query("SELECT persona, element FROM %s WHERE id = 1"%text,
                      conn)

    s = pd.read_sql_query("SELECT persona, element FROM events WHERE id = 1", conn)
    print(d)
    print(s)
    # cur = conn.cursor()
    # cur.execute("SELECT * FROM events "
    #             "LIMIT 10")

    # print(d)

    # desc = cur.description
    # column_names = [col[0] for col in desc]
    # print(column_names)
    # sql_dict = [dict(zip(row, column_names[0])) for row in cur.fetchall()]
    # print(sql_dict)

    # row = cur.fetchone()
    # title, starts = row

    # print(title)
    # print(starts)

def create_dict(conn, table):
    """Create a dictionary by hospital_id as the key from pre-existing
    hopital_info table with remaining columns as dictionary values."""

    d = pd.read_sql_query("SELECT * FROM %s" % table, conn)
    sql_dict = d.set_index("id").to_dict('index')

    return sql_dict


test_connection("events")
print(create_dict(connect_to_sql(), "events"))
print(connect_to_sql())