"""
Driver file to generate weekly reports using interactive dashboard

Run by typing 'streamlit run weekly-report.py 2022-10-21' in terminal
Where '2022-10-21' can be any yyyy-mm-dd, indicating collection date

Authors: Carol Ling     <caroll2@andrew.cmu.edu>
#        Xiaochen Sun   <xsun3@andrew.cmu.edu>
#        Xiaonuo Xu     <xiaonuox@andrew.cmu.edu>
"""

import sys
import datetime
import warnings
import streamlit as st
import numpy as np
import datetime as time
import matplotlib.pyplot as plt
from getdata import connect_to_sql2
from getdata import get_records_number, get_beds_detail, get_beds_sum_by, \
                    get_covid_change, get_previous_weeks

# Ignore the warning message from the code
warnings.filterwarnings("ignore")

# Display all columns from pandas data frame
# pd.set_option('display.max_columns', None)

collect_date = str(sys.argv[1])
collect_date = datetime.datetime.strptime(collect_date, "%Y-%m-%d").date()
last_week = collect_date - time.timedelta(days=7)

# Create connection object
conn = connect_to_sql2()
cur = conn.cursor()

# Begin generating analysis

# Title of the report
title = "Hospital Beds and COVID Cases Report for Week " + str(collect_date)
st.title(title)

# Part 1
st.header("1. Hospital Records Loaded")

st.markdown("A summary of how many hospital records were loaded in the most" +
            "recent week, and how that compares to previous weeks.")

st.subheader("Health and Human Services (HHS) Data")
record_number = get_records_number(conn, "hospital_data", collect_date)

if not record_number:
    st.text("Server lacks HHS data on " + str(collect_date))
else:
    for key, value in record_number.items():
        st.text("PostgreSQL server contains " + str(value) +
                " HHS records from " + str(key))

# Part 2
st.header("2. Hospital Beds Available and in Use")

st.markdown("A table summarizing the number of adult and pediatric beds " +
            "available this week, the number used, and the number used by " +
            "patients with COVID, compared to the 4 most recent weeks.")

bed_recent = get_beds_detail(conn, collect_date, True)

if bed_recent is False:
    st.text("Server lacks HHS data on " + str(collect_date))
else:
    bed_recent = bed_recent.iloc[:, [0, 1, 3, 4, 5, -2]]
    bed_recent = bed_recent.set_index('collection_date')
    bed_recent.rename(columns={'avalible_adult_beds': 'Avaliable Adult Beds',
                               'avalible_pediatric_beds':
                               'Avaliable Pediatric Beds',
                               'occupied_adult_beds': 'Occupied Adult Beds',
                               'occupied_pediatric_beds':
                               'Occupied Pediatric Beds',
                               'occupied_icu_beds': 'Occupied ICU Beds'},
                      inplace=True)
    st.dataframe(bed_recent)

st.header("3. Hospital Beds Information by Quality Rating")

st.markdown("A table summarizing the number of beds in use by different " +
            "hospital quality rating, so we can compare groups of " +
            "high-quality and low-quality hospitals.")

bed_by_quality = get_beds_sum_by(conn, collect_date, "quality_rating")

if bed_by_quality is False:
    st.text("Server lacks HHS and CMS data on " + str(collect_date))
else:
    bed_by_quality.rename(columns={'occupied_adult_beds':
                                   'Occupied Adult Beds',
                                   'occupied_pediatric_beds':
                                   'Occupied Pediatric Beds',
                                   'occupied_icu_beds':
                                   'Occupied ICU Beds',
                                   'covid_beds_use': 'COVID Bed Use',
                                   'quality_rating': 'Quality Rating'},
                          inplace=True)
    bed_by_quality = bed_by_quality.iloc[:, 3:8]
    bed_by_quality = bed_by_quality.round(0).astype('Int64')
    cols = [4, 0, 1, 2, 3]
    bed_by_quality = bed_by_quality[[bed_by_quality.columns[i] for i in cols]]
    st.dataframe(bed_by_quality)

st.header("4. Hospital Bed Use by All Cases versus COVID-Only Over Time")

st.markdown("A plot of the total number of hospital beds used " +
            "per week, over all time, split into all cases " +
            "and COVID cases.")

bed_all_time = get_beds_detail(conn, collect_date, False)

if bed_all_time is False:
    st.text("Server lacks HHS data on " + str(collect_date))
else:
    bed_all_time = bed_all_time.iloc[:, 3:]
    bed_all_time = bed_all_time.set_index('collection_date')
    xtick = list(bed_all_time.index)
    xtick = sorted([date.strftime("%y-%m-%d") for date in xtick])
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    plt.plot(xtick, bed_all_time["covid_beds_use"], label="COVID Only")
    plt.plot(xtick, bed_all_time["covid_beds_use"] +
             bed_all_time["non_covid_beds_use"], label="Total")
    ax.set_xticklabels(sorted(xtick), rotation=45, ha="right")
    ax.set_ylim(bottom=0)
    plt.title("Cumulative Sum of all Hospital Bed Usage per Week")
    plt.xlabel("Date (YY-MM-DD)")
    plt.ylabel("Cumulative Beds in Use")
    plt.fill_between(
        x=xtick,
        y1=bed_all_time["covid_beds_use"],
        color="#1F77B4",
        alpha=0.4)
    plt.fill_between(
        x=xtick,
        y1=bed_all_time["covid_beds_use"] + bed_all_time["non_covid_beds_use"],
        color="#FF7F0E",
        alpha=0.2)
    fig.tight_layout()
    plt.legend()
    st.write(fig)

st.header("5. Hospital Utilization by Type of Hospital Ownership")

st.markdown("Graphs of hospital utilization (the percent of " +
            "available beds being used) by type of hospital " +
            "(private or public), over time." + "\n" +
            "There is a problem with the adult utilization data, " +
            "as it is pulling from the wrong column (it is coverage " +
            "instead of usage).")

listofdate = get_previous_weeks(cur, "hospital_data", collect_date)

if listofdate is False:
    st.text("Server lacks HHS and CMS data on " + str(collect_date))
else:
    list = get_previous_weeks(cur, "hospital_data", collect_date)
    list = sorted(list)[-4:]

    for i in list:
        bed_by_ownership = get_beds_sum_by(conn, i, "ownership")
        bed_by_ownership = bed_by_ownership.iloc[:, 7:]

        labels = bed_by_ownership["ownership"]
        adult_util = bed_by_ownership["adult_utilization"]
        ped_util = bed_by_ownership["pediatric_utilization"]
        icu_util = bed_by_ownership["icu_utilization"]
        x = np.arange(len(labels))
        width = 0.2
        fig = plt.figure()

        plt.bar(x-0.2, adult_util, width, color='cyan',
                label='Adult Utilization')
        plt.bar(x, ped_util, width, color='orange',
                label='Pediatric Utilization')
        plt.bar(x+0.2, icu_util, width, color='green',
                label='ICU Utilization')
        plt.xticks(x, labels=labels, rotation=45, ha="right")
        plt.ylabel('Proportion')
        plt.title('Hospital Utilization of Different Hospital Ownership on ' +
                  str(i))
        plt.legend()
        st.write(fig)


st.header("6. Rank States by Change in COVID-19 Case Since Last Week")

st.markdown("A table of the states in which the number of cases " +
            "has increased by the most since last week.")

state_rank = get_covid_change(conn, collect_date, 10, "state")

if state_rank is False:
    st.text("Server lacks HHS and CMS data on " + str(collect_date))
else:
    state_rank.rename(columns={'state': 'State',
                               'change_covid_bed_use':
                               'Change in COVID Bed Use',
                               'covid_' + str(last_week):
                               'Total COVID Cases (' + str(last_week) + ')',
                               'covid_' + str(collect_date):
                               'Total COVID Cases (' + str(collect_date) + ')'
                               }, inplace=True)
    cols = [3, 0, 1, 2]
    state_rank = state_rank[[state_rank.columns[i] for i in cols]]
    st.dataframe(state_rank)

st.header("7. Rank Hospital by Change in COVID Cases Since Last Week")

st.markdown("A table of the hospitals (including names and locations) " +
            "with the largest absolute changes in COVID cases in the last " +
            "week.")

hospital_rank = get_covid_change(conn, collect_date, 10, "hospital_id")

if hospital_rank is False:
    st.text("Server lacks HHS and CMS data on " + str(collect_date))
else:
    hospital_rank.rename(columns={'hospital_id': 'Hospital ID',
                                  'change_covid_bed_use':
                                  'Absolute Change in COVID Bed Use',
                                  'covid_' + str(last_week):
                                  'Total COVID Cases ('+str(last_week)+')',
                                  'covid_cases':
                                  'Total COVID Cases ('+str(collect_date)+')',
                                  'name': 'Name',
                                  'city': 'City',
                                  'state': 'State',
                                  'zip': 'ZIP',
                                  'address': 'Address'},
                         inplace=True)
    cols = [0, 8, 7, 1, 2, 3, 4, 5]
    hospital_rank = hospital_rank[[hospital_rank.columns[i] for i in cols]]
    st.dataframe(hospital_rank)

st.text("Made by Team Pipers, 2022")

# Close the connection to psql server
cur.close()
conn.close()
