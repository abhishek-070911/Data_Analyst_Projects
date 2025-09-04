/* ---------- Creating  Database ---------- */
CREATE DATABASE Electric_Vehicles
USE Electric_Vehicles


/* ---------- Creating Table ---------- */
CREATE TABLE EV
(
`Year` INT,
Month_Name VARCHAR(30),
`Date` DATE,
State VARCHAR(100),
Vehicle_Class VARCHAR(200),
Vehicle_Category VARCHAR(200),
Vehicle_Type VARCHAR(200),
EV_Sales_Quantity INT
)


/* ---------- Loading the Dataset ---------- */
load data infile 'D:/Prot/Data Analyst/UM selected projects/I_Electric Vehicles/Electric Vehicle Sales by State in India_Edited.csv'
into table EV
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;



/* ---------- Cleaning the dataset ---------- */

/* ---------- Trimming extra spaces if present ---------- */
UPDATE EV
SET 
	Month_Name = TRIM(Month_Name),
    State = TRIM(State),
    Vehicle_Class = TRIM(Vehicle_Class),
    Vehicle_Category = TRIM(Vehicle_Category),
    Vehicle_Type = TRIM(Vehicle_Type);
    

/* ---------- Querying the dataset after loading ---------- */
/* ---------- Querying the dataset after loading ---------- */
SELECT * FROM EV;


/* ---------- Filtering all rows where Ev Sales Quantity > 100 ---------- */
SELECT * FROM EV WHERE EV_Sales_Quantity > 100 ORDER BY EV_Sales_Quantity DESC;


/* ---------- Finding all unique Vehicle Type and Vehicle Class ---------- */
SELECT DISTINCT Vehicle_Class FROM EV;
SELECT DISTINCT Vehicle_Category FROM EV;


/* ---------- Querying for total EV sales for each year ---------- */
SELECT `Year`,SUM(EV_Sales_Quantity) AS Total_Sales FROM EV GROUP BY `Year` ORDER BY `Year`;


/* ---------- Querying sales for each month and year ---------- */
SELECT `Year`,Month_Name,SUM(EV_Sales_Quantity) AS Total_Sales FROM EV GROUP BY `Year`,Month_Name ORDER BY `Year`,Total_Sales DESC;


/* ---------- Querying for average sales per state ---------- */
SELECT State,ROUND(AVG(EV_Sales_Quantity),2) AS Average_Sales FROM EV GROUP BY State ORDER BY Average_Sales DESC;


/* ---------- Querying for total sales per state per year ---------- */
SELECT `Year`,State,SUM(EV_Sales_Quantity) AS Total_Sales FROM EV GROUP BY `Year`,State ORDER BY `Year`,Total_Sales DESC;


/* ---------- Querying EV sales by Vehicle_Category ---------- */
SELECT Vehicle_Category,SUM(EV_Sales_Quantity) AS Total_Sales FROM EV GROUP BY Vehicle_Category ORDER BY Total_Sales DESC;


/* ---------- Querying states whose sales are above the national average ---------- */
SELECT State,SUM(EV_Sales_Quantity) AS Total_Sales FROM EV GROUP BY State HAVING SUM(EV_Sales_Quantity) >
(SELECT AVG(State_Sales) FROM (SELECT SUM(EV_Sales_Quantity) AS State_Sales FROM EV GROUP BY State) AS SUB) 
ORDER BY Total_Sales DESC;


/* ---------- Ranking states by EV sales for each year ---------- */
SELECT `Year`,State,SUM(EV_Sales_Quantity) AS Total_Sales, DENSE_RANK() OVER (PARTITION BY `Year` ORDER BY `Year`,SUM(EV_Sales_Quantity) DESC) 
AS Ranking FROM EV GROUP BY `Year`,State;


/* ---------- Running total of EV sales month by month ---------- */
SELECT `Year`,Month_Name,State,SUM(EV_Sales_Quantity) AS Monthly_Sales,SUM(SUM(EV_Sales_Quantity))
OVER(PARTITION BY `Year`,State ORDER BY `Date`) AS Running_Total
FROM EV GROUP BY `Year`,Month_Name,State,`Date` ORDER BY State,`Year`,`Date`;


/* ---------- Querying for Year-over-Year sales growth per state ---------- */
WITH yearly_sales AS
(
SELECT `Year`,State,SUM(EV_Sales_Quantity) AS Total_Sales FROM EV GROUP BY `Year`,State
)
SELECT `Year`,State,Total_Sales,LAG(Total_Sales) OVER(PARTITION BY State ORDER BY `Year`) AS Previous_sales,
ROUND(
(Total_Sales -LAG(Total_Sales) OVER(PARTITION BY State ORDER BY `Year`)/LAG(Total_Sales) OVER(PARTITION BY State ORDER BY `Year`))*100,2
) AS yoy_growth_percent FROM yearly_sales ORDER BY `Year`,State;


/* ---------- Calculating sales share (%) of each state in a given year ---------- */
WITH state_sales AS
(
SELECT `Year`,State,SUM(EV_Sales_Quantity) AS Total_Sales FROM EV GROUP BY `Year`,State
),
national_sales AS
(
SELECT `Year`,SUM(Total_Sales) AS National_Sales FROM state_sales GROUP BY `Year`
)
SELECT s.`Year`,s.State,s.Total_Sales,t.National_Sales,
ROUND((s.Total_Sales*100/t.National_Sales),2) AS Sales_share_percent
FROM state_sales s INNER JOIN national_sales t ON s.`Year` = t.`Year` ORDER BY Sales_share_percent DESC;


/* ---------- Identifying the top 3 vehicle types sold in each year ---------- */
SELECT * FROM (SELECT `Year`,Vehicle_Type,SUM(EV_Sales_Quantity) AS Total_Sales,
DENSE_RANK() OVER(PARTITION BY `Year` ORDER BY SUM(EV_Sales_Quantity) DESC) AS Top_3_sales
FROM EV GROUP BY `Year`,Vehicle_Type) AS Sales WHERE Top_3_sales IN (1,2,3) ;


/* ---------- Converting monthly sales into columns (Jan, Feb,........,Dec) for a given year ---------- */
SELECT `Year`,
SUM(CASE WHEN Month_Name = 'January' THEN EV_Sales_Quantity ELSE 0 END) AS January_Sales,
SUM(CASE WHEN Month_Name = 'February' THEN EV_Sales_Quantity ELSE 0 END) AS February_Sales,
SUM(CASE WHEN Month_Name = 'March' THEN EV_Sales_Quantity ELSE 0 END) AS March_Sales,
SUM(CASE WHEN Month_Name = 'April' THEN EV_Sales_Quantity ELSE 0 END) AS April_Sales,
SUM(CASE WHEN Month_Name = 'May' THEN EV_Sales_Quantity ELSE 0 END) AS May_Sales,
SUM(CASE WHEN Month_Name = 'June' THEN EV_Sales_Quantity ELSE 0 END) AS June_Sales,
SUM(CASE WHEN Month_Name = 'July' THEN EV_Sales_Quantity ELSE 0 END) AS July_Sales,
SUM(CASE WHEN Month_Name = 'August' THEN EV_Sales_Quantity ELSE 0 END) AS August_Sales,
SUM(CASE WHEN Month_Name = 'September' THEN EV_Sales_Quantity ELSE 0 END) AS September_Sales,
SUM(CASE WHEN Month_Name = 'October' THEN EV_Sales_Quantity ELSE 0 END) AS October_Sales,
SUM(CASE WHEN Month_Name = 'November' THEN EV_Sales_Quantity ELSE 0 END) AS November_Sales,
SUM(CASE WHEN Month_Name = 'December' THEN EV_Sales_Quantity ELSE 0 END) AS December_Sales
FROM EV GROUP BY `Year` ORDER BY `Year`;


/* ---------- Writing a procedure to return sales trends for a given state & year ---------- */
DELIMITER $$
CREATE PROCEDURE Sales_Trend_By_State_Year
(
IN State_Name VARCHAR(100),
IN Sale_Year INT
)
BEGIN
		SELECT `Year`,Month_Name,SUM(EV_Sales_Quantity) AS Monthly_Sales FROM EV 
        WHERE State = State_Name AND `Year` = Sale_Year GROUP BY  `Year`,Month_Name
        ORDER BY  `Year`,Month_Name;
END $$
DELIMITER $$;

CALL Sales_Trend_By_State_Year('Odisha',2021);


/* ---------- Market share of each vehicle class per year ---------- */
WITH class_sales AS (
SELECT `Year`,Vehicle_Class,SUM(EV_Sales_Quantity) AS Class_Sales FROM EV GROUP BY `Year`,Vehicle_Class
),
yearly_total_sales AS(
SELECT `Year`,SUM(Class_Sales) AS Total_Sales FROM class_sales GROUP BY `Year`
)
SELECT c.`Year`,c.Vehicle_Class,c.Class_Sales,y.Total_Sales,
ROUND((c.Class_Sales*100/y.Total_Sales),2) AS Market_Share_Percentage
FROM class_sales c INNER JOIN yearly_total_sales y
ON c.`Year` = y.`Year`
ORDER BY c.`Year`,Market_Share_Percentage DESC;




SELECT * FROM EV




