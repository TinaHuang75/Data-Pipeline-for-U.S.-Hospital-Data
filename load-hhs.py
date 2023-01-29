"""This module is for loading the HHS weekly data. The data updates the
schema on sculptor"""

import psycopg
import sys
import credentials
import pandas as pd
import numpy as np
import csv


filename = sys.argv[1]

hhs = pd.read_csv(filename)

# Clean negatives
hhs["all_adult_hospital_beds_7_day_avg"] = \
    hhs["all_adult_hospital_beds_7_day_avg"].replace(-999999, np.NaN)
hhs["all_pediatric_inpatient_beds_7_day_avg"] = \
    hhs["all_pediatric_inpatient_beds_7_day_avg"].replace(-999999, np.NaN)
hhs["all_adult_hospital_inpatient_bed_occupied_7_day_coverage"] = \
    hhs["all_adult_hospital_inpatient_bed_occupied_7_day_coverage"].\
    replace(-999999, np.NaN)
hhs["all_pediatric_inpatient_bed_occupied_7_day_avg"] = \
    hhs["all_pediatric_inpatient_bed_occupied_7_day_avg"].\
    replace(-999999, np.NaN)
hhs["total_icu_beds_7_day_avg"] = \
    hhs["total_icu_beds_7_day_avg"].replace(-999999, np.NaN)
hhs["icu_beds_used_7_day_avg"] = \
    hhs["icu_beds_used_7_day_avg"].replace(-999999, np.NaN)
hhs["inpatient_beds_used_covid_7_day_avg"] = \
    hhs["inpatient_beds_used_covid_7_day_avg"].replace(-999999, np.NaN)
hhs["staffed_icu_adult_patients_confirmed_covid_7_day_avg"] = \
    hhs["staffed_icu_adult_patients_confirmed_covid_7_day_avg"].\
    replace(-999999, np.NaN)

# change nan values to None
hhs.replace(np.NaN, None, inplace=True)

# create a set to store error message
error = set()

# connect to the remote machine
conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname=credentials.DB_USER,
    user=credentials.DB_USER, password=credentials.DB_PASSWORD
)

cur = conn.cursor()

num_rows_inserted = 0

with conn.transaction():
    for row in hhs.itertuples():
        # columns for hospital table
        hospital_pk = row.hospital_pk
        hospital_name = row.hospital_name
        address = row.address
        state = row.state
        city = row.city
        zip = row.zip
        Fips_code = row.fips_code
        geo_coded_hospital_address = row.geocoded_hospital_address
        collection_date = row.collection_week
        adult_beds_weekly_avg = row.all_adult_hospital_beds_7_day_avg
        pediatric_beds_weekly_avg = row.all_pediatric_inpatient_beds_7_day_avg
        adult_beds_occupied_weekly_avg = \
            row.all_adult_hospital_inpatient_bed_occupied_7_day_coverage
        pediatric_beds_occupied_weekly_avg = \
            row.all_pediatric_inpatient_bed_occupied_7_day_avg
        icu_beds_weekly_avg = row.total_icu_beds_7_day_avg
        icu_beds_used_weekly_avg = row.icu_beds_used_7_day_avg
        patients_confirmed_covid_weekly_avg = \
            row.inpatient_beds_used_covid_7_day_avg
        adult_icu_patients_confirmed_covid_weekly_avg = \
            row.staffed_icu_adult_patients_confirmed_covid_7_day_avg

        # deal with geo_coded_hospital_address
        if isinstance(geo_coded_hospital_address, str):
            x = geo_coded_hospital_address.find("(")
            y = geo_coded_hospital_address.find(")")
            geo_coded_hospital_address = geo_coded_hospital_address[x+1:y]
            loc = geo_coded_hospital_address.split(" ")
            geo_coded_hospital_address = str(loc[0]) + "," + str(loc[1])
        else:
            geo_coded_hospital_address = None

        # Create an id for beds and patients table
        # which is the combinaiton of hospital_pk and collection_date
        id = hospital_pk + collection_date

        # insert into hospital table
        try:
            with conn.transaction():
                # insert values into the hospital table
                cur.execute("INSERT INTO hospital(hospital_pk, hospital_name, \
                            address, state, city, zip, Fips_code, \
                            geo_coded_hospital_address)"
                            "values (%s, %s, %s, %s, %s, %s, %s, %s)"
                            "on conflict (hospital_pk) DO UPDATE "
                            "set Fips_code = %s, \
                            geo_coded_hospital_address = %s",
                            (hospital_pk, hospital_name, address, state, city,
                             zip, Fips_code, geo_coded_hospital_address,
                             Fips_code, geo_coded_hospital_address))
        except Exception as e:
            # if an exception/error happens in this block, Postgres goes back
            # to the last savepoint upon exiting the `with` block
            row_num = row.Index + 1
            print("insert failed for hospital table in row " + str(row_num)
                  + ": " + repr(e))
            # add the row that has insertion error to the error set
            error.add(row)

        try:
            with conn.transaction():
                # insert values into beds table
                cur.execute("INSERT INTO beds(beds_id, hospital_pk, collection_date, \
                            adult_beds_weekly_avg, \
                            pediatric_beds_weekly_avg, \
                            adult_beds_occupied_weekly_avg, \
                            pediatric_beds_occupied_weekly_avg, \
                            icu_beds_weekly_avg, icu_beds_used_weekly_avg)"
                            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            "on conflict (beds_id) DO UPDATE "
                            "set adult_beds_weekly_avg = %s, \
                            pediatric_beds_weekly_avg = %s, \
                            adult_beds_occupied_weekly_avg = %s, \
                            pediatric_beds_occupied_weekly_avg = %s, \
                            icu_beds_weekly_avg = %s, \
                            icu_beds_used_weekly_avg = %s",
                            (id, hospital_pk, collection_date,
                             adult_beds_weekly_avg,
                             pediatric_beds_weekly_avg,
                             adult_beds_occupied_weekly_avg,
                             pediatric_beds_occupied_weekly_avg,
                             icu_beds_weekly_avg, icu_beds_used_weekly_avg,
                             adult_beds_weekly_avg,
                             pediatric_beds_weekly_avg,
                             adult_beds_occupied_weekly_avg,
                             pediatric_beds_occupied_weekly_avg,
                             icu_beds_weekly_avg, icu_beds_used_weekly_avg))
        except Exception as e:
            # if an exception/error happens in this block, Postgres goes back
            # to the last savepoint upon exiting the `with` block
            row_num = row.Index + 1
            print("insert failed for beds table in row " + str(row_num) + ": "
                  + repr(e))
            # add the row that has insertion error to the error set
            error.add(row)

        try:
            with conn.transaction():
                # insert values into patients table
                cur.execute(
                    "INSERT INTO Patients(patients_ID, hospital_pk, \
                    collection_date, \
                    patients_confirmed_covid_weekly_avg, \
                    adult_icu_patients_confirmed_covid_weekly_avg)"
                    "values (%s, %s, %s, %s, %s)"
                    "on conflict (patients_ID) DO UPDATE "
                    "set patients_confirmed_covid_weekly_avg = %s, \
                    adult_icu_patients_confirmed_covid_weekly_avg = %s",
                    (id, hospital_pk, collection_date,
                     patients_confirmed_covid_weekly_avg,
                     adult_icu_patients_confirmed_covid_weekly_avg,
                     patients_confirmed_covid_weekly_avg,
                     adult_icu_patients_confirmed_covid_weekly_avg))
        except Exception as e:
            # if an exception/error happens in this block, Postgres goes back
            # to the last savepoint upon exiting the `with` block
            row_num = row.Index + 1
            print("insert failed for patients table in row " + str(row_num) +
                  ": " + repr(e))
            # add the row that has insertion error to the error set
            error.add(row)
        else:
            # no exception happened, so we continue without
            # reverting the savepoint
            num_rows_inserted += 1

# write the error message to a csv file
file = open("erroneous-rows_hhs.csv", "w+", newline="\n")
with file:
    write = csv.writer(file)
    # write column names to the file
    write.writerow(["index"] + list(hhs.columns))
    # write error rows to the file
    write.writerows(list(error))

# commit and close the connection
conn.commit()
conn.close()
