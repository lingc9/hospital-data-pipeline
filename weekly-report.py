"""
Driver file to generate weekly reports

Authors: Carol Ling     <caroll2@andrew.cmu.edu>
#        Xiaochen Sun   <xsun3@andrew.cmu.edu>
#        Xiaonuo Xu     <xiaonuox@andrew.cmu.edu>
"""

import sys
import datetime
import warnings
import pandas as pd
from getdata import connect_to_sql2
from getdata import get_records_number, get_beds_detail, get_beds_sum_by, \
                    get_covid_change

# Ignore the warning message from the code
warnings.filterwarnings("ignore")

# Display all columns from pandas data frame
pd.set_option('display.max_columns', None)

collect_date = str(sys.argv[1])
collect_date = datetime.datetime.strptime(collect_date, "%Y-%m-%d").date()

# Create connection object
conn = connect_to_sql2()
cur = conn.cursor()
print("Generating report for " + str(collect_date) + ":")

# Begin generating analysis

print("\n 1. Hospital records loaded in the recent weeks \n")
record_number = get_records_number(conn, "hospital_data", collect_date)

if not record_number:
    print("Server lacks HHS data on " + str(collect_date))
else:
    for key, value in record_number.items():
        print("PostgreSQL server contains " + str(value) +
              " HHS records from " + str(key))

print("\n 2. Number of beds available and in use in recent weeks \n")
bed_recent = get_beds_detail(conn, collect_date, True)
bed_recent = bed_recent.iloc[:, [0, 1, 3, 4, 5]]

if bed_recent is False:
    print("Server lacks HHS data on " + str(collect_date))
else:
    print(bed_recent)

print("\n 3. Number of beds in use this week by quality rating \n")
bed_by_quality = get_beds_sum_by(conn, collect_date, "quality_rating")
bed_by_quality = bed_by_quality.iloc[:, 3:8]

if bed_by_quality is False:
    print("Server lacks HHS and CMS data on " + str(collect_date))
else:
    print(bed_by_quality)

print("\n 4. Hospital bed use by all cases and covid of all time \n")
bed_all_time = get_beds_detail(conn, collect_date, False)
bed_all_time = bed_all_time.iloc[:, 3:]

if bed_all_time is False:
    print("Server lacks HHS data on " + str(collect_date))
else:
    print(bed_all_time)

print("\n 5. Hospital utilization by type of hospital ownership \n")
bed_by_ownership = get_beds_sum_by(conn, collect_date, "ownership")
bed_by_ownership = bed_by_ownership.iloc[:, 7:]

if bed_by_ownership is False:
    print("Server lacks HHS and CMS data on " + str(collect_date))
else:
    print(bed_by_ownership)

print("\n 6. Rank states by change in covid case since last week \n")
state_rank = get_covid_change(conn, collect_date, 10, "state")

if state_rank is False:
    print("Server lacks HHS and CMS data on " + str(collect_date))
else:
    print(state_rank)

print("\n 7. Rank hospital by change in covid case since last week \n")

hospital_rank = get_covid_change(conn, collect_date, 10, "hospital_id")

if hospital_rank is False:
    print("Server lacks HHS and CMS data on " + str(collect_date))
else:
    print(hospital_rank)

# Close the connection to psql server
cur.close()
conn.close()
