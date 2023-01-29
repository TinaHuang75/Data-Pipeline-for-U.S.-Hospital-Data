--The basic entity for this table is Hospital. 
--Ten entries are: hospital_pk, hospital_name, Hospital_Type, Hospital_Ownership, 
--Emergency_Service, address, state, city, zip, fips_code and geo_coded_hospital_address. 
--These columns summarize the names, types, ownerships, services and locations of the hospitals.
--We combined these attributes in one table since they are unlikely to change overtime. 
--Since the attribute hospital_pk contains unique IDs for the hospitals. 
--We use that as the primary key of the table. 

create table Hospital (
	hospital_pk text PRIMARY KEY, 
	hospital_name text NOT NULL, 
	address text, 
	state varchar(2),
	city text, 
	zip varchar(10), 
	Fips_code text,
	geo_coded_hospital_address point, 
	Hospital_Type text,
	Hospital_Ownership text, 
	Emergency_Service boolean
); 

--The entity in this table is beds. 
--The entries in the entity are the beds_id, hospital_pk, collection_date, adult_beds_weekly_avg,
--pediatric_beds_weekly_avg, adult_beds_occupied_weekly_avg, pediatric_beds_occupied_weekly_avg, 
--icu_beds_weekly_avg, icu_beds_used_weekly_avg. 
--These columns look at the weekly data associated with the number of beds in various hospitals 
--as well as how many of those beds are occupied, across pediatric and adult patients. 
--Most of the columns in this table will be updating weekly, 
--so it is easier to update the beds data all at once in the one entity.

CREATE TABLE beds (
	beds_id text PRIMARY KEY,
    hospital_pk text,
	FOREIGN KEY (hospital_pk) REFERENCES Hospital(hospital_pk) MATCH FULL,
	collection_date DATE,
	adult_beds_weekly_avg numeric,
    pediatric_beds_weekly_avg numeric,
    adult_beds_occupied_weekly_avg numeric,
    pediatric_beds_occupied_weekly_avg numeric,
	icu_beds_weekly_avg numeric,
    icu_beds_used_weekly_avg numeric
);

--The main entity in this table is patients. 
--The entries for this table are patients_id, hospital_pk, collection_date, 
--patients_confirmed_covid_weekly_avg, adult_ice_patients_confirmed_covid_weekly_avg. 
--This table looks at how many patients that have covid in a given hospital and the number of adults 
--that have confirmed covid. Once again, this will be changing weekly, so we wanted to have this entity 
--separate from some of the other entities that will not be updating as frequently.

CREATE TABLE Patients (
	patients_ID text PRIMARY KEY,
    Hospital_pk text,
	FOREIGN KEY (hospital_pk) REFERENCES Hospital(hospital_pk) MATCH FULL,
	Collection_date DATE,
	patients_confirmed_covid_weekly_avg numeric,
	adult_icu_patients_confirmed_covid_weekly_avg numeric
);


--The basic entity is the quality_rating. Since this quality is collected quarterly, 
--we put it in a separate table where we could update this table while there are new records. 
--It contains a rate_id which indicates each quality_rating record. 
--Hospital_id is the foreign key related Hospital_id in the Hospital Table. 
--Collection_Date is the date when the quality_rating was collected. 
--Quality_rating is just the actual quality_rating.


create table Rating (
	rate_id text PRIMARY KEY,
	Hospital_pk text,
	FOREIGN KEY (Hospital_pk) REFERENCES Hospital(Hospital_pk) MATCH FULL,
	Collection_Date date,
	quality_rating numeric
	);



