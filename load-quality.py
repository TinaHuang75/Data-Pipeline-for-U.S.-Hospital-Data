"""This module is for loading the quality data. The data updates the
schema on sculptor"""

import psycopg
import sys
import credentials
import pandas as pd
from datetime import datetime
import numpy as np
import csv

collection_date = sys.argv[1]
filename = sys.argv[2]

# Read the dataset
# rating = pd.read_csv("Hospital_General_Information-2021-07.csv")
rating = pd.read_csv(filename)
# Convert space in column names into underscore
rating.columns = rating.columns.str.replace(' ', '_')

# Clean up the data
rating['Hospital_overall_rating'] = rating['Hospital_overall_rating'].\
    replace('Not Available', '-1')
rating['Hospital_overall_rating'] = \
    pd.to_numeric(rating['Hospital_overall_rating'])

# change nan values to None
rating.replace(np.nan, None, inplace=True)

# convert collection_date to a datetime object
collection_date_asDate = datetime.strptime(collection_date, '%Y-%m-%d').date()

# Connect to the remote machine
conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname=credentials.DB_USER,
    user=credentials.DB_USER, password=credentials.DB_PASSWORD
)

# create a set to store error message
error = set()

cur = conn.cursor()

num_rows_inserted = 0

with conn.transaction():
    for row in rating.itertuples():
        hospital_pk = row.Facility_ID
        hospital_type = row.Hospital_Type
        hospital_name = row.Facility_Name
        hospital_ownership = row.Hospital_Ownership
        emergency_service = row.Emergency_Services
        quality_rating = row.Hospital_overall_rating
        address = row.Address
        state = row.State
        city = row.City
        zip = row.ZIP_Code

        # Create an id for rating table
        # which is the combinaiton of hospital_pk and collection_date
        id = hospital_pk + collection_date

        # deal with the emergency_avail, yes to True, no to False
        # otherwise Make it NULL
        if emergency_service == 'Yes':
            emergency_service = True
        elif emergency_service == 'No':
            emergency_service = False
        else:
            emergency_service = None

        # insert into hospital table
        # if the hospital_pk already exit, just update type, ownership
        # and Emergency_Service
        try:
            with conn.transaction():
                cur.execute("insert into hospital (hospital_pk, hospital_name, \
                            city, state, address, zip, hospital_type, \
                            hospital_ownership, Emergency_Service) "
                            "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            "on conflict (hospital_pk) DO UPDATE "
                            "set hospital_type = %s, hospital_ownership = %s, \
                                Emergency_Service = %s",
                            (hospital_pk, hospital_name, city,
                             state, address, zip, hospital_type,
                             hospital_ownership, emergency_service,
                             hospital_type, hospital_ownership,
                             emergency_service))
        except Exception as e:
            # if an exception/error happens in this block, Postgres goes back
            # to the last savepoint upon exiting the `with` block
            row_num = row.Index + 1
            print("insert failed for hospital table in row " + str(row_num) +
                  ": " + repr(e))
            # add the row that has insertion error to the error set
            error.add(row)

        try:
            with conn.transaction():
                cur.execute("INSERT INTO rating(rate_id, hospital_pk, quality_rating,\
                            collection_date)"
                            "values (%s, %s, %s, %s)"
                            "on conflict (rate_ID) DO UPDATE "
                            "set quality_rating = %s",
                            (id, hospital_pk, quality_rating,
                                collection_date_asDate, quality_rating))
        except Exception as e:
            # if an exception/error happens in this block, Postgres goes back
            # to the last savepoint upon exiting the `with` block
            row_num = row.Index + 1
            print("insert failed for rating table in row " + str(row_num) +
                  ": " + repr(e))
            # add the row that has insertion error to the error set
            error.add(row)
        else:
            # no exception happened, so we continue without reverting the
            # savepoint
            num_rows_inserted += 1

# write the error message to a csv file
file = open("erroneous-rows_quality.csv", "w+", newline="\n")
with file:
    write = csv.writer(file)
    # write column names to the file
    write.writerow(["index"] + list(rating.columns))
    # write error rows to the file
    write.writerows(list(error))

# commit and close the connection
conn.commit()
conn.close()
