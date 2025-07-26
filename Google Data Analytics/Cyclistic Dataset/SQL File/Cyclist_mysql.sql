/* ---------- Creating  Database ---------- */
CREATE DATABASE google_project
USE google_project


/* ---------- Creating Table ---------- */
CREATE TABLE cyclist
(
ride_id varchar(50),
rideable_type varchar(50),
started_at varchar(50),
ended_at varchar(50),
start_station_name varchar(50),
start_station_id varchar(50),
end_station_name varchar(50),
end_station_id varchar(50),
start_lat float,
start_lng float,
end_lat float,
end_lng float,
member_casual varchar(50),
ride_length time,
day_of_week int
)


/* ---------- Removes all active SQL modes  ---------- */
SET SESSION sql_mode = ''



/* ---------- Loading the Dataset ---------- */

load data infile 
"D:/Prot/Data Analyst/Google Analytics/Capestone Projects/First_case_study_19-05-25/CSV Files/my_dataset.csv"
into table cyclist 
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows 


/* ---------- Querying the dataset ---------- */
SELECT * FROM cyclist;


/* ---------- Querying for different types of rides used by customers and their total count ---------- */
SELECT rideable_type,COUNT(rideable_type) AS number_of_customers FROM cyclist GROUP BY rideable_type


/* ---------- Querying for total number of members and casuals ---------- */
SELECT member_casual, COUNT(member_casual) AS number_of_customers FROM cyclist GROUP BY member_casual


/* ---------- Querying for maximum time taken for a ride by customers ---------- */
SELECT MAX(ride_length) AS Max_length_of_a_ride FROM cyclist


/* ---------- Querying for minimum time taken for a ride by customers ---------- */
SELECT MIN(ride_length) AS Min_length_of_a_ride FROM cyclist


/* ---------- Querying for percentage of different customers using different types of bikes ---------- */
SELECT member_casual, rideable_type,COUNT(rideable_type) AS No_of_customer,
ROUND(COUNT(rideable_type)*100/(select count(*) from cyclist),2) AS Percent_of_customers FROM
cyclist GROUP BY member_casual, rideable_type ORDER BY 1,2


/* ---------- Querying for percentage of different customers using bikes on day basis ---------- */
SELECT member_casual,day_of_week,COUNT(day_of_week) AS No_of_customer,
ROUND(COUNT(day_of_week)*100/(select count(*) from cyclist),2) AS Percent_of_customers
FROM cyclist GROUP BY member_casual,day_of_week ORDER BY 1,2
