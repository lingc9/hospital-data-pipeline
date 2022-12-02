"""
Driver file to load and clean Hospital General Information data.

Authors: Carol Ling     <caroll2@andrew.cmu.edu>
#        Xiaochen Sun   <xsun3@andrew.cmu.edu>
#        Xiaonuo Xu     <xiaonuox@andrew.cmu.edu>
"""

import sys
import time
import datetime
import warnings
from tqdm import tqdm
import pandas as pd
from cleandata import clean_quality_data
from loaddata import connect_to_sql, load_hospital_info, count_hospitals

# Ignore the warning message from the code
warnings.filterwarnings("ignore")

nfile = "./data/hospital_quality/" + str(sys.argv[2])
insert_data = clean_quality_data(nfile)

collect_date = str(sys.argv[1])
collect_date = datetime.datetime.strptime(collect_date, "%Y-%m-%d")

# Subset data to insert (Testing Purposes)
# insert_data = insert_data.iloc[0:500, ]

print("Detected " + str(len(insert_data)) + " rows of data")

# Start Insertion
num_rows_inserted = 0
failed_insertion = []
conn = connect_to_sql()
cur = conn.cursor()

# Count number of hospitals before insertion
count_before = count_hospitals(cur, "hospital_info")

# Start Timer
start = time.time()

with conn.transaction():
    print("Connection established, begin inserting the data...")
    for i in tqdm(range(insert_data.shape[0])):
        data = insert_data.loc[int(i), ]
        try:
            with conn.transaction():
                load_hospital_info(cur, data, collect_date)
        except Exception:
            failed_insertion.append(i)
            print(data)
            raise Exception("Insertion failed at line " + str(i))
        else:
            num_rows_inserted += 1

# Count number of hospitals after insertion
new_hospital = count_hospitals(cur, "hospital_info") - count_before
rows_updated = num_rows_inserted - new_hospital

# Stop timer
end = time.time()
elapsed = round(end - start, 2)

print("Time elapsed: " + str(elapsed) + " seconds")
print("Read in " + str(insert_data.shape[0]) + " rows of data in total")
print("Successfully added " + str(new_hospital) + " new hospitals")
print("Successfully updated " + str(rows_updated) + " existing hospitals")

# Output csv with lines that failed to insert
if failed_insertion:
    orginal_df = pd.read_csv(nfile)
    failed_lines = orginal_df.iloc[failed_insertion]
    curr_time = time.strftime("%H_%M_%S", time.localtime())
    fname = "./data/hospital_quality/" + curr_time + "_failed_insertion.csv"
    failed_lines.to_csv(fname)
    print("Saved lines that failed to insert in " + fname)

# Commit the changes to psql server
cur.close()
conn.commit()
conn.close()
