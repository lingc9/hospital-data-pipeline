"""
This is a test function for postgresql server connection

Delete this file from the project once done
"""

from loaddata import connect_to_sql


def test_connection():
    conn = connect_to_sql()
    cur = conn.cursor()
    cur.execute("SELECT title, starts FROM social_events "
                "LIMIT 10")

    row = cur.fetchone()
    title, starts = row

    print(title)
    print(starts)


test_connection()
