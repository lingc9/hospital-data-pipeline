"""
Driver file to generate weekly reports

Authors: Carol Ling     <caroll2@andrew.cmu.edu>
#        Xiaochen Sun   <xsun3@andrew.cmu.edu>
#        Xiaonuo Xu     <xiaonuox@andrew.cmu.edu>
"""

import sys
import datetime
import warnings
from getdata import connect_to_sql2
from getdata import get_records_number, get_beds_detail

# Ignore the warning message from the code
warnings.filterwarnings("ignore")

collect_date = str(sys.argv[1])
collect_date = datetime.datetime.strptime(collect_date, "%Y-%m-%d").date()

# Create connection object
conn = connect_to_sql2()
cur = conn.cursor()
print("Generating report for " + str(collect_date) + ":")

record_number = get_records_number(cur, "hospital_data", collect_date)

if record_number:
    pass
else:
    print("not exist")

boo = get_beds_detail(conn, collect_date)
print(boo)

# Close the connection to psql server
cur.close()
conn.close()
