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
load data infile 'D:/Prot/Data Analyst/Data Analysis/I_Road Accident/Road Accident.csv'
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


/* ---------- Checking the grain of the table ---------- */
/* One row = one city x Cause_Category x Cause_Subcategory x Outcome_of_Incident.
   The 6 Cause Categories are 6 different ways of splitting the SAME accidents: for every city,
   each category adds up to the same totals. Adding rows across categories therefore counts every
   accident 6 times, and adding across outcomes mixes accidents with people killed or injured
   ('Total Injured' already includes the two injury rows).
   Rule used in every query below: compute each total inside ONE Cause_Category and ONE outcome. */
SELECT City_Name,Cause_Category,SUM(Count_in_mil) AS Total_Accidents FROM Accidents
WHERE Outcome_of_Incident = 'Total number of Accidents'
GROUP BY City_Name,Cause_Category ORDER BY City_Name,Cause_Category;
-- Every city shows the same total in all 6 categories (Chennai: 4389 each time).


/* ---------- Querying the dataset after loading ---------- */
/* ---------- Querying the dataset after loading ---------- */
SELECT * FROM Accidents;


/* ---------- Retrieving distinct city names ---------- */
SELECT DISTINCT City_Name FROM Accidents;


/* ---------- Querying all unique Cause Category values ---------- */
SELECT DISTINCT Cause_Category FROM Accidents;


/* ---------- Finding all records where Persons were killed in accidents ---------- */
SELECT * FROM Accidents WHERE Outcome_of_Incident = 'Persons Killed' AND Count_in_mil > 0 ORDER BY Count_in_mil DESC;


/* ---------- Querying total accidents per city and ranking them ---------- */
SELECT City_Name,SUM(Count_in_mil) AS Total_Accidents,DENSE_RANK() OVER(ORDER BY SUM(Count_in_mil) DESC) AS Accident_Rank
FROM Accidents WHERE Outcome_of_Incident = 'Total number of Accidents' AND Cause_Category = 'Traffic Violation'
GROUP BY City_Name ORDER BY Accident_Rank;


/* ---------- Calculating the share of accidents for each Cause Subcategory within its Cause Category ---------- */
-- This replaces "average accident per Cause Category". Categories can't be compared with each other,
-- because each one splits the same accidents; a higher average only meant fewer subcategories.
SELECT Cause_Category,Cause_Subcategory,SUM(Count_in_mil) AS Total_Accidents,
ROUND(SUM(Count_in_mil)*100/SUM(SUM(Count_in_mil)) OVER(PARTITION BY Cause_Category),2) AS Percent_of_Category
FROM Accidents WHERE Outcome_of_Incident = 'Total number of Accidents'
GROUP BY Cause_Category,Cause_Subcategory ORDER BY Cause_Category,Total_Accidents DESC;


/* ---------- Calculating the totals per Outcome of Incident ---------- */
-- Accidents and people are different units, so each outcome is its own total.
-- 'Total Injured' = 'Greviously Injured' + 'Minor Injury'.
SELECT Outcome_of_Incident,SUM(Count_in_mil) AS Total FROM Accidents WHERE Cause_Category = 'Traffic Violation'
GROUP BY Outcome_of_Incident ORDER BY Total DESC;


/* ---------- Creating a pivot: Cities vs. Outcomes, showing total counts in each outcome type ---------- */
SELECT City_Name,
	SUM(CASE WHEN Outcome_of_Incident = 'Greviously Injured' THEN Count_in_mil ELSE 0 END) AS Greviously_Injured,
    SUM(CASE WHEN Outcome_of_Incident = 'Minor Injury' THEN Count_in_mil ELSE 0 END) AS Minor_Injury,
    SUM(CASE WHEN Outcome_of_Incident = 'Persons Killed' THEN Count_in_mil ELSE 0 END) AS Persons_Killed,
    SUM(CASE WHEN Outcome_of_Incident = 'Total Injured' THEN Count_in_mil ELSE 0 END) AS Total_Injured,
    SUM(CASE WHEN Outcome_of_Incident = 'Total number of Accidents' THEN Count_in_mil ELSE 0 END) AS Total_number_of_Accidents
FROM Accidents WHERE Cause_Category = 'Traffic Violation' GROUP BY City_Name ORDER BY City_Name;


/* ---------- Finding cities with accident count greater than the average city accident count ---------- */
WITH city_accidents AS
(
SELECT City_Name,SUM(Count_in_mil) AS Total_Accidents FROM Accidents
WHERE Outcome_of_Incident = 'Total number of Accidents' AND Cause_Category = 'Traffic Violation'
GROUP BY City_Name
)
SELECT City_Name,Total_Accidents FROM city_accidents
WHERE Total_Accidents > (SELECT AVG(Total_Accidents) FROM city_accidents) ORDER BY Total_Accidents DESC;


/* ---------- Getting the second highest accident count city ---------- */
SELECT * FROM
(SELECT City_Name, SUM(Count_in_mil) AS Total_Accidents, DENSE_RANK() OVER(ORDER BY SUM(Count_in_mil) DESC) AS 'Rank'
FROM Accidents WHERE Outcome_of_Incident = 'Total number of Accidents' AND Cause_Category = 'Traffic Violation'
GROUP BY City_Name) AS SUB WHERE `Rank` = 2;


/* ---------- Calculating accidents per city, then finding the percentage contribution of each city to the national total ---------- */
WITH acc_per_city AS
(
SELECT City_Name,SUM(Count_in_mil) AS Total_Accidents FROM Accidents
WHERE Outcome_of_Incident = 'Total number of Accidents' AND Cause_Category = 'Traffic Violation'
GROUP BY City_Name
)
SELECT City_Name,Total_Accidents,ROUND((Total_Accidents*100/(SELECT SUM(Total_Accidents) FROM acc_per_city)),2) AS Percent
FROM acc_per_city ORDER BY Percent DESC;


/* ---------- Identifying the most dangerous cause (Cause_Subcategory) in each Cause Category, by deaths across all cities ---------- */
-- 'Others' appears in every category, so each subcategory is kept together with its category.
-- In Traffic Violation, the label 'Over' is over-speeding (the source file cut the text at the hyphen).
WITH deaths_by_cause AS
(
SELECT Cause_Category,Cause_Subcategory,SUM(Count_in_mil) AS Persons_Killed,
RANK() OVER(PARTITION BY Cause_Category ORDER BY SUM(Count_in_mil) DESC) AS Danger_Rank
FROM Accidents WHERE Outcome_of_Incident = 'Persons Killed'
GROUP BY Cause_Category,Cause_Subcategory
)
SELECT Cause_Category,Cause_Subcategory,Persons_Killed FROM deaths_by_cause WHERE Danger_Rank = 1 ORDER BY Persons_Killed DESC;
