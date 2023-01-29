# Hospital_database

This is an informative file detailing how to use our two modules: load-hhs.py 
and load-quality.py for inserting the weekly HHS data and the CMS Quality
data into our database schema.

# load-hhs.py

For this module, all that is needed is access to the cvs that you want to
insert, and the module. From the command line, you will run in the
following exact order

python load-hhs.py <filename.csv>

This will insert the data into the tables in our schema. It will also provide
useful summary information after running, such as the number of rows loaded and 
the number of new hospitals that were found in the dataset. In addition, there 
will be a csv file output with the row number and the error message for all
of the rows that present issues. From there, you can make the necessary 
changes and resubmit the edited error csv in the same way as 
before into the schema.

# load-quality.py

For this module, all that is needed is access to the cvs that you want to
insert, knowledge of the current date and the module. From the command line,
you will run in the follwing exact order

python load-quality.py YYYY-MM-DD <filename.csv>

This will insert the data into the tables in our schema. It will also provide
useful summary information after running, such as the number of rows loaded and 
the number of new hospitals that were found in the dataset. In addition, there 
will be a csv file output with the row number and the error message for all
of the rows that present issues. From there, you can make the necessary 
changes and resubmit the edited error csv in the same way as 
before into the schema.

# Dashboard Report 

Along with the PSQL tables that we created, you can also generate a dashboard by running
the commands below: 

papermill Report.ipynb YYYY-MM-DD-report.ipynb -p week YYYY-MM-DD

jupyter nbconvert --no-input --to html YYYY-MM-DD-report.ipynb  

in which YYYY-MM-DD is the date associated with the csv files for HHS databases. 

The dashboard will be generated as an html file named YYYY-MM-DD-report.html, which contains tables and graphs as listed below: 

1. A summary of how many hospital records were loaded in the most recent week, and how that compares to previous 4 weeks.
2. A table summarizing the number of adult and pediatric beds available this week, the number used, and the number used by
patients with COVID, compared to the 4 most recent weeks. 
3. A bar plot or table summarizing the fraction of beds currently in use by hospital quality rating. 
4. A bar plot of the total number of hospital beds used per week, over all time, split into all cases and COVID cases. 
5. A map showing the number of COVID cases by state for the most recent week (this map is interactive. You can see the 
number of covid cases of a state by moving your mouse on the map). 
6. A table of the states in which the number of cases has increased by the most since last week. 
7. A bar plot of hospital utilization (the percent of available beds being used) by type of hospital (private or public), over time. 

The jupyter notebook for creating this dashboard is named weekly-report.ipynb, which you can find in this GitHub repositroy.
