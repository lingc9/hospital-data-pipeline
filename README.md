# Hospital Database

# Description

The reposity contains data folder, database tables creation file and other important python files about loading data, cleaning data and testing. Two data files sources from the US Department of Health and Human Services and the Centers for Medicare and Medicaid Services. Data folder include all data files we used, cleandata.py used to changing the data useless or missing, load-hhs.py is used to update HSS data, load-quality.py is used to update quality data, loaddata.py is used to insert the data in sql, and two testing python file are used to test the connection between python and sql and the geocode function.

# Instructions

schema.sql:  
The SQL schema. Run first to create three tables in the PostgreSQL environment.

cleandata.py:  
clean_quality_data - Converts and cleans hospital data from Centers for Medicare and Medicaid Services (CMS).  
clean_hhs_data - Converts and cleans general hospital data from the US Department of Health and Human Services (HHS).  

load-hhs.py:  
Driver file to load cleaned weekly HHS data into PostgreSQL when given a csv file from HHS.  
Outputs a csv with lines that failed to insert.

Run with the following command line prompt:
  python load-hhs.py file_name.csv

Example call:
  python load-hhs.py 2022-01-04-hhs-data.csv

load-quality.py:  
Driver file to load cleaned Hospital General Information data into PostgreSQL when given a csv file from CMS.
Outputs a csv with lines that failed to insert.

Run with the following command line prompt:
  python load-quality.py YYYY-MM-DD file_name.csv

Example call:
  python load-quality.py 2021-07-01 Hospital_General_Information-2021-07.csv

loaddata.py:  
connect_to_sql - Creates a connection to Postgresql.
load_hospital_data - Inserts a row of data into hospital_data.
load_hospital_info - Inserts or updates a row of data into hospital_info.  
load_hospital_location - Inserts or updates a row of data into hospital_location.  
count_hospitals - Counts number of hospitals in relations where each hospital_id is unique.
