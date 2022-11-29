# Hospital Database

# Description

The reposity contains data folder, database tables creation file and other important python files about loading data, cleaning data and testing. Two data files sources from the US Department of Health and Human Services and the Centers for Medicare and Medicaid Services. Data folder include all data files we used, cleandata.py used to changing the data useless or missing, load-hhs.py is used to update HSS data, load-quality.py is used to update quality data, loaddata.py is used to insert the data in sql, and two testing python file are used to test the connection between python and sql and the geocode function.

# Instructions

schema.sql:  
The first step to create three tables in a sql environment.

cleandata.py:  
format_geocode() is used to find the longitude and latitude separately of the hospital.  
clean_quality_data - converts and cleans hospital general information data.  
file_path - directory to where the csv file is stored, in string  
clean_hhs_data - converts and cleans hospital general information data  

load -hhs.py:  
Driver file to load and clean weekly HHS data.  
Output csv with lines that failed to insert  

load-quality.py:  
Driver file to load and clean Hospital General Information data.  

loaddata.py:  
connect_to_sql - creats a connection to postgresql  
load_hospital_data - insert a row of data into hospital_data  
load_hospital_info - insert or update a row of data into hospital_info  
load_hospital_location - insert or update a row of data into hospital_location  
update_hospital_info - update a hospital's inforamtion in hospital_info  
update_hospital_location - update a hospital's location in hospital_location  
insert_hospital_info - insert a new hospital to hospital_info  
insert_hospital_location - insert a new hospital to hospital_location  
check_hospital_info - check if a hospital exist in hospital_info  
check_hospital_location - check if a hospital exist in hospital_location  

dict_info - dictionary containing all hospital id from hospital_info  
dict_location - dictionary containing all hospital id from hospital_location  
conn - psycopg connect to postgresql server  
data - a row of pd data frame containing the data to be inserted  
collect_date - the date when the quality file is collected  
