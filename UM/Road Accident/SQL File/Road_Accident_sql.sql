/* ---------- Creating  Database ---------- */
CREATE DATABASE Road_Accidents;
USE Road_Accidents;


/* ---------- Creating Table ---------- */
CREATE TABLE Accidents
(
City_Name VARCHAR(50),
Cause_Category VARCHAR(50),
Cause_Subcategory VARCHAR(50),
Outcome_of_Incident VARCHAR(50),
Count_in_mil INT
);


/* ---------- Loading the Dataset ---------- */
load data infile 'D:/Prot/Data Analyst/UM selected projects/I_Road Accident/Road Accident.csv'
into table Accidents
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;


/* ---------- Cleaning the dataset ---------- */

/* ---------- Trimming extra spaces if present ---------- */
UPDATE Accidents
SET
	City_Name = TRIM(City_Name),
    Cause_Category = TRIM(Cause_Category),
    Cause_Subcategory = TRIM(Cause_Subcategory),
    Outcome_of_Incident = TRIM(Outcome_of_Incident);


/* ---------- Querying the dataset after loading ---------- */
/* ---------- Querying the dataset after loading ---------- */
SELECT * FROM Accidents;


/* ---------- Retrieving distinct city names ---------- */
SELECT DISTINCT City_Name FROM Accidents;


/* ---------- Querying all unique Cause Category values ---------- */
SELECT DISTINCT Cause_Category FROM Accidents;


/* ---------- Finding all records where Persons were killed in accidents ---------- */
SELECT * FROM Accidents WHERE Outcome_of_Incident = 'Persons Killed' AND Count_in_mil > 1 ORDER BY Count_in_mil DESC;


/* ---------- Querying total accidents per city and ranking them ---------- */
SELECT City_Name,SUM(Count_in_mil) AS Total_Accidents,DENSE_RANK() OVER(ORDER BY SUM(Count_in_mil) DESC) AS Accident_Rank
FROM Accidents GROUP BY City_Name;


/* ---------- Calculating the average accident per Cause Category ---------- */
SELECT Cause_Category,ROUND(AVG(Count_in_mil),2) AS Average_Accidents FROM Accidents GROUP BY Cause_Category ORDER BY Average_Accidents DESC;


/* ---------- Calculating the total accidents per Outcome of Incident ---------- */
SELECT Outcome_of_Incident,SUM(Count_in_mil) AS Total_Accidents FROM Accidents GROUP BY Outcome_of_Incident ORDER BY Total_Accidents DESC;


/* ---------- Creating a pivot: Cities vs. Outcomes, showing total counts in each outcome type ---------- */
SELECT City_Name,
	SUM(CASE WHEN Outcome_of_Incident = 'Greviously Injured' THEN Count_in_mil ELSE 0 END) AS Greviously_Injured,
    SUM(CASE WHEN Outcome_of_Incident = 'Minor Injury' THEN Count_in_mil ELSE 0 END) AS Minor_Injury,
    SUM(CASE WHEN Outcome_of_Incident = 'Persons Killed' THEN Count_in_mil ELSE 0 END) AS Persons_Killed,
    SUM(CASE WHEN Outcome_of_Incident = 'Total Injured' THEN Count_in_mil ELSE 0 END) AS Total_Injured,
    SUM(CASE WHEN Outcome_of_Incident = 'Total number of Accidents' THEN Count_in_mil ELSE 0 END) AS Total_number_of_Accidents
FROM Accidents GROUP BY City_Name ORDER BY City_Name;


/* ---------- Finding cities with accident count greater than the average city accident count ---------- */
SELECT City_Name,SUM(Count_in_mil) AS Total_Accidents FROM Accidents GROUP BY City_Name 
HAVING Total_Accidents > (SELECT AVG(Count_in_mil) AS Average_Accident FROM Accidents);


/* ---------- Getting the second highest accident count city ---------- */
SELECT * FROM
(SELECT City_Name, SUM(Count_in_mil) AS Total_Accidents, DENSE_RANK() OVER(ORDER BY SUM(Count_in_mil) DESC) AS 'Rank'
FROM Accidents GROUP BY City_Name) AS SUB WHERE `Rank` = 2;


/* ---------- Calculating accidents per city, then finding the percentage contribution of each city to the national total ---------- */
WITH acc_per_city AS
(
SELECT City_Name,SUM(Count_in_mil) AS Total_Accidents FROM Accidents GROUP BY City_Name
)
SELECT City_Name,Total_Accidents,ROUND((Total_Accidents*100/(SELECT SUM(Count_in_mil) FROM Accidents)),2) AS Percent 
FROM acc_per_city GROUP BY City_Name,Total_Accidents ORDER BY Percent DESC;


/* ---------- Identifying the most dangerous cause (Cause_Subcategory) across all cities ---------- */
SELECT City_Name,Cause_Subcategory,SUM(Count_in_mil) AS Total_Accident FROM Accidents
GROUP BY City_Name,Cause_Subcategory ORDER BY Total_Accident DESC;