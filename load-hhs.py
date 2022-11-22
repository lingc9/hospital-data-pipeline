"""Driver file to load and clean weekly HHS data."""

import sys
import time
import warnings
import pandas as pd
from cleandata import clean_hhs_data
from loaddata import connect_to_sql, load_hospital_data, \
                     load_hospital_location, count_hospitals

# Ignore the warning message from the code
warnings.filterwarnings("ignore")

nfile = "./data/hhs_weekly/" + str(sys.argv[1])
insert_data = clean_hhs_data(nfile)

# Subset data to insert (Testing Purposes)
# insert_data = insert_data.iloc[0:100, ]

print("Detected " + str(len(insert_data)) + " rows of data")

# Start Insertion
num_rows_inserted = 0
failed_insertion_data = []
failed_insertion_location = []
conn = connect_to_sql()
cur = conn.cursor()

# Count number of hospitals before insertion
count_before = count_hospitals(cur, "hospital_location")

# Start Timer
start = time.time()

with conn.transaction():
    print("Connection established, begin inserting the data...")
    for i in range(insert_data.shape[0]):
        data = insert_data.loc[int(i), ]
        try:
            with conn.transaction():
                load_hospital_data(cur, data)
        except Exception:
            failed_insertion_data.append(i)
            print("Insertion into hospital_data failed at line " + str(i))
        try:
            with conn.transaction():
                tmp = load_hospital_location(cur, data)
        except Exception:
            failed_insertion_location.append(i)
            print("Insertion into hospital_location failed at line " + str(i))
        else:
            num_rows_inserted += 1

# Count number of hospitals after insertion
new_hospital = count_hospitals(cur, "hospital_location") - count_before
rows_updated = num_rows_inserted - new_hospital

# Stop timer
end = time.time()
elapsed = round(end - start, 2)

print("Time elapsed: " + str(elapsed) + " seconds")
print("Read in " + str(insert_data.shape[0]) + " rows of data in total")
print("Successfully added " + str(new_hospital) + " new hospitals")
print("Successfully updated " + str(rows_updated) + " existing hospitals")

# Output csv with lines that failed to insert
if failed_insertion_data:
    orginal_df = pd.read_csv(nfile)
    failed_lines = orginal_df.iloc[failed_insertion_data]
    curr_time = time.strftime("%H_%M_%S", time.localtime())
    fname = "./data/hhs_weekly/" + curr_time + "_failed_insertion_data.csv"
    failed_lines.to_csv(fname)
    print("Saved lines that failed to insert in " + fname)

if failed_insertion_location:
    orginal_df = pd.read_csv(nfile)
    failed_lines = orginal_df.iloc[failed_insertion_location]
    curr_time = time.strftime("%H_%M_%S", time.localtime())
    fname = "./data/hhs_weekly/" + curr_time + "_failed_insertion_loca.csv"
    failed_lines.to_csv(fname)
    print("Saved lines that failed to insert in " + fname)

# Commit the changes to psql server
cur.close()
conn.commit()
conn.close()
