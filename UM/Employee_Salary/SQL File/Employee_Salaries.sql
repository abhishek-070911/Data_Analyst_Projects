/* ---------- Using Database ---------- */
USE PROJECTS_UM;


/* ---------- Creating Table ---------- */
CREATE TABLE EMPLOYEE
(
Employee_Name VARCHAR(50),
Job_Title VARCHAR(50),
Base_Pay FLOAT,
Overtime_Pay FLOAT,
Other_Pay FLOAT,
Total_Pay_Without_Benefits FLOAT,
Benefits FLOAT,
Total_Pay_Benefits FLOAT,
`Year` INT
)


/* ---------- Loading the Dataset ---------- */
load data infile 'D:/Prot/Data Analyst/UM selected projects/A_Employee Salaries/Employee_Salaries_cleaned.csv'
into table EMPLOYEE
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;


/* ---------- Cleaning the dataset ---------- */

/* ---------- Trimming extra spaces if present ---------- */
UPDATE EMPLOYEE
SET 
	Employee_Name = TRIM(Employee_Name),
    Job_Title = TRIM(Job_Title);



/* ---------- Querying the dataset after loading ---------- */
/* ---------- Querying the dataset after loading ---------- */
SELECT * FROM EMPLOYEE;



/* ---------- Counting total employees ---------- */
SELECT COUNT(*) FROM EMPLOYEE;


/* ---------- Querying for distinct job titles and its count ---------- */
SELECT DISTINCT Job_Title FROM EMPLOYEE;
SELECT COUNT(DISTINCT Job_Title) FROM EMPLOYEE;

/* ---------- Querying for total pay benefits ---------- */
SELECT * FROM EMPLOYEE WHERE Total_Pay_Benefits < 10000 ORDER BY Total_Pay_Benefits DESC;


/* ---------- Querying for Top 10 highest paid employee  ---------- */
SELECT Employee_Name,`Year`,Total_Pay_Benefits FROM EMPLOYEE ORDER BY Total_Pay_Benefits DESC LIMIT 10;


/* ---------- Querying for Average pay benefits per job title  ---------- */
SELECT Job_Title,AVG(Total_Pay_Benefits) AS Average_Benefit FROM EMPLOYEE GROUP BY Job_Title ORDER BY Average_Benefit DESC;


/* ---------- Querying for Total pay benefits per job title  ---------- */
SELECT Job_Title,SUM(Total_Pay_Benefits) AS Total_Pay_Benefit FROM EMPLOYEE GROUP BY Job_Title ORDER BY Total_Pay_Benefit DESC;


/* ---------- Querying for number of employees per job title  ---------- */
SELECT Job_Title,COUNT(*) AS Total_employee FROM EMPLOYEE GROUP BY Job_Title ORDER BY Total_employee DESC;


/* ---------- Querying for employees hired after 2015  ---------- */
SELECT * FROM EMPLOYEE WHERE `Year` > 2015 ORDER BY `Year`;


/* ---------- Querying for employees earning below average benefits  ---------- */
SELECT * FROM EMPLOYEE WHERE Total_Pay_Benefits < (SELECT AVG(Total_Pay_Benefits) FROM EMPLOYEE) ORDER BY Total_Pay_Benefits


/* ---------- Creating Pay Benefits distribution bucket  ---------- */
SELECT CASE 
    WHEN Total_Pay_Benefits < 10000 THEN 'Low Pay'
    WHEN Total_Pay_Benefits > 10001 AND Total_Pay_Benefits < 100000 THEN 'Average Pay'
	WHEN Total_Pay_Benefits > 100001 AND Total_Pay_Benefits < 200000 THEN 'Well Pay'
    ELSE 'High pay'
END AS 'Benefits_Bucket',COUNT(*) AS Employee_count
FROM EMPLOYEE GROUP BY Benefits_Bucket


/* ---------- Year wise hiring trend  ---------- */
SELECT `Year`,COUNT(*) AS Total_hired FROM EMPLOYEE GROUP BY `Year` ORDER BY `Year`


/* ---------- Assign a Row Number to Each Employee by Job_Title ---------- */
SELECT Employee_Name,Job_Title,Total_Pay_Benefits , ROW_NUMBER() OVER(PARTITION BY Job_Title ORDER BY Total_Pay_Benefits DESC) AS Deptt_Rank FROM EMPLOYEE


/* ---------- Querying for Top paid employee per job title  ---------- */
SELECT * FROM (SELECT *,DENSE_RANK() OVER(PARTITION BY Job_Title ORDER BY Total_Pay_Benefits DESC) AS Top_paid_employee FROM EMPLOYEE) AS EMP
 WHERE Top_paid_employee IN (1,2,3)


/* ---------- Showing Previous Employee’s Benefits ---------- */
SELECT Employee_Name,Job_Title,Total_Pay_Benefits,`Year`,LAG(Total_Pay_Benefits) OVER(PARTITION BY Job_Title ORDER BY Total_Pay_Benefits DESC) AS 
Previous_Employee_Salary FROM EMPLOYEE


/* ---------- Calculating difference in benefits ---------- */
SELECT Employee_Name,Job_Title,Total_Pay_Benefits,`Year`,
Total_Pay_Benefits - LAG(Total_Pay_Benefits) OVER(PARTITION BY Job_Title ORDER BY Total_Pay_Benefits DESC) AS Benefits_Difference
FROM EMPLOYEE

/* ---------- Merging Both ---------- */
SELECT Employee_Name,Job_Title,Total_Pay_Benefits,`Year`,LAG(Total_Pay_Benefits) OVER(PARTITION BY Job_Title ORDER BY Total_Pay_Benefits DESC) AS 
Previous_Employee_Salary, Total_Pay_Benefits- LAG(Total_Pay_Benefits) 
OVER(PARTITION BY Job_Title ORDER BY Total_Pay_Benefits DESC) AS Benefits_Difference
FROM EMPLOYEE


/* ---------- Finding Cumulative benefits ---------- */
SELECT Employee_Name,Job_Title,Total_Pay_Benefits, SUM(Total_Pay_Benefits) OVER(PARTITION BY Job_Title ORDER BY Total_Pay_Benefits  ) AS 
Cumulative_Benefits FROM EMPLOYEE


/* ---------- Compare Each Employee Salary with Job title Average ---------- */
SELECT Employee_Name,Job_Title,Total_Pay_Benefits, AVG(Total_Pay_Benefits) OVER(PARTITION BY Job_Title ) AS 
Job_Avg_Benefit FROM EMPLOYEE ORDER BY Job_Title


/* ---------- Difference of Each Employee Salary with Job title Average ---------- */
SELECT Employee_Name,Job_Title,Total_Pay_Benefits, AVG(Total_Pay_Benefits) OVER(PARTITION BY Job_Title ) AS Job_Avg_Benefit,
Total_Pay_Benefits - AVG(Total_Pay_Benefits) OVER(PARTITION BY Job_Title ) AS Benefit_Difference FROM EMPLOYEE ORDER BY Job_Title

/* ---------- Dividing Employees into Pay benefits Quartiles Within Job title ---------- */
SELECT Employee_Name,Job_Title,Total_Pay_Benefits, NTILE(4) OVER(PARTITION BY Job_Title ORDER BY Total_Pay_Benefits) AS 
Benefits_quartile FROM EMPLOYEE


SELECT * FROM EMPLOYEE