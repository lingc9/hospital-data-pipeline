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
connect_to_sql - Creates a connection to Postgresql using psycopg.
load_hospital_data - Inserts a row of data into hospital_data.
load_hospital_info - Inserts or updates a row of data into hospital_info.  
load_hospital_location - Inserts or updates a row of data into hospital_location.  
count_hospitals - Counts number of hospitals in relations where each hospital_id is unique.

weekly-report.py:  
Driver file to generate report on HHS and CMS data in a given week, using the Information from PostgreSQL server.
Opens a dashboard in the browser. Streamlit package is necessary. 

To install streamlit:
  pip install streamlit

Run with the following command line prompt:
  streamlit run weekly-report.py YYYY-MM-DD

Example call:
  streamlit run weekly-report.py 2022-10-07

console-dashboard.py:  
Driver file to generate report on HHS and CMS data in a given week, using the Information from PostgreSQL server.
Prints out the tables from the report in the console. It's the alternative to using streamlit.

Run with the following command line prompt:
  python console-dashboard.py YYYY-MM-DD

Example call:
  python console-dashboard.py 2022-10-07

getdata.py:  
connect_to_sql2 - Creates a connection to Postgresql using psycopg2.
get_distinct_collection_date - Returns of list of all unique collection dates in a SQL table. 
get_previous_weeks - Returns of list of all unique collection dates before the given date in a SQL table. 
get_records_number - Returns a dictionary containing number of records loaded in the most recent weeks
get_beds_detail - Returns a table of hospital beds information from the most recents weeks or from every week in SQL database, grouped by collection date
get_beds_sum_by - Returns a table of hospital beds information in a given week, grouped by general quality rating, or state, or hospital ownership (e.g. public/private).
get_hospital - Returns a table of hospital beds information in a given week, grouped by hospital id.
get_covid_change - Returns a table ranking hospitals or states in terms of the absolute change in covid cases from last week to the given week.