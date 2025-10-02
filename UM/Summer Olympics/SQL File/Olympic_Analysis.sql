/* ---------- Creating  Database ---------- */
CREATE DATABASE Olympics;
USE Olympics;


/* ---------- Creating Table ---------- */
CREATE TABLE Summer_Olympics
(
City VARCHAR(30),
`Year` INT,
Sport VARCHAR(30),
Discipline VARCHAR(30),
`Event` VARCHAR(100),
Athlete VARCHAR(100),
Gender VARCHAR(10),
Country_Code VARCHAR(10),
Country VARCHAR(100),
Event_gender VARCHAR(10),
Medal VARCHAR(100)
);


/* ---------- Loading the Dataset ---------- */
load data infile 'D:/Prot/Data Analyst/UM selected projects/B_Olympics/Summer-Olympic-medals-1976-to-2008_cleaned.csv'
into table Summer_Olympics
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;


/* ---------- Cleaning the dataset ---------- */

/* ---------- Trimming extra spaces if present ---------- */
UPDATE Summer_Olympics
SET
	City = TRIM(City),
    Sport = TRIM(Sport),
    Discipline = TRIM(Discipline),
    `Event` = TRIM(`Event`),
    Athlete = TRIM(Athlete),
    Gender = TRIM(Gender),
    Country_Code = TRIM(Country_Code),
    Country = TRIM(Country),
    Event_gender = TRIM(Event_gender),
    `Medal` = TRIM(`Medal`);
    
    
/* ---------- Querying the dataset after loading ---------- */
/* ---------- Querying the dataset after loading ---------- */
SELECT * FROM Summer_Olympics;


/* ---------- Dropping unwanted columns ---------- */
ALTER TABLE Summer_Olympics
DROP COLUMN Country_Code,
DROP COLUMN Event_gender;


/* ---------- Querying for athletes who won gold medal ---------- */
SELECT * FROM Summer_Olympics WHERE `Medal` = 'Gold';


/* ---------- Querying for distinct sports ---------- */
SELECT DISTINCT Sport FROM Summer_Olympics;


/* ---------- Querying for 2000 Sydney sports ---------- */
SELECT * FROM Summer_Olympics WHERE `Year` = '2000' AND City = 'Sydney';


/* ---------- Querying for total medals won by USA ---------- */
SELECT COUNT(*) FROM Summer_Olympics WHERE Country = 'United States' AND `Medal` = 'Gold';


/* ---------- Querying for female athletes who won medals ---------- */
SELECT * FROM Summer_Olympics WHERE Discipline = 'Athletics' AND Gender = 'Women';


/* ---------- Querying for top 5 countries with total medal count ---------- */
SELECT Country,COUNT('Medal') AS Total_Medals FROM Summer_Olympics GROUP BY Country ORDER BY Total_Medals DESC LIMIT 5;


/* ---------- Querying for medals won each year by India ---------- */
SELECT `Year`,COUNT('Medal') AS Total_Medals FROM Summer_Olympics WHERE Country = 'India' GROUP BY `Year`  ORDER BY `Year`;


/* ---------- Querying for medals based on gender ---------- */
SELECT Gender,COUNT('Medal') AS Total_Medals FROM Summer_Olympics GROUP BY Gender;


/* ---------- Querying for most successful athlete ---------- */
SELECT Athlete,COUNT('Medal') AS Total_Medals,DENSE_RANK() OVER(ORDER BY COUNT('Medal') DESC) AS Top_Athlete FROM Summer_Olympics GROUP BY Athlete
LIMIT 1;


/* ---------- Querying for sport with highest number of events ---------- */
SELECT Sport, COUNT('Event') AS Total_Events FROM Summer_Olympics GROUP BY Sport ORDER BY Total_Events DESC;


/* ---------- Ranking countries by total medals for each year ---------- */
SELECT Country,COUNT('Medal') AS Total_Medals, DENSE_RANK() OVER(ORDER BY COUNT('Medal') DESC) AS `Rank` FROM Summer_Olympics
GROUP BY Country;


/* ---------- Finding athletes who have won medals in multiple Olympics (different years) ---------- */
SELECT Athlete,COUNT(DISTINCT `Year`) AS Olympics_Participated FROM Summer_Olympics
WHERE Medal IS NOT NULL GROUP BY Athlete HAVING COUNT(DISTINCT `Year`) > 1
ORDER BY Olympics_Participated DESC;


/* ---------- Finding the country that won the most Gold medals in each year ---------- */
SELECT `Year`,Country,COUNT(Medal) AS Most_Gold FROM Summer_Olympics WHERE Medal = 'Gold' GROUP BY `Year`,Country ORDER BY `Year`,Country;


/* ---------- Creating a pivot-style output: Year vs Gold/Silver/Bronze medals by USA ---------- */
SELECT `Year`,
		SUM(CASE WHEN Medal = 'Gold' THEN 1 ELSE 0 END) AS GOLD,
        SUM(CASE WHEN Medal = 'Silver' THEN 1 ELSE 0 END) AS SILVER,
        SUM(CASE WHEN Medal = 'Bronze' THEN 1 ELSE 0 END) AS BRONZE
FROM Summer_Olympics WHERE Country = 'United States' GROUP BY `Year` ORDER BY `Year`;


/* ---------- Identifying sports where women have won more medals than men ---------- */
SELECT Sport,
	SUM(CASE WHEN Gender = 'Women' THEN 1 ELSE 0 END) AS Women_Medals,
    SUM(CASE WHEN Gender = 'Men' THEN 1 ELSE 0 END) AS Men_Medals
FROM Summer_Olympics 
WHERE Medal IS NOT NULL
GROUP BY Sport HAVING SUM(CASE WHEN Gender = 'Women' THEN 1 ELSE 0 END) > SUM(CASE WHEN Gender = 'Men' THEN 1 ELSE 0 END)
ORDER BY Women_Medals DESC;