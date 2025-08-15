/* ---------- Creating  Database ---------- */
CREATE DATABASE TCS_Stock_Analysis
USE TCS_Stock_Analysis


/* ---------- Creating Table ---------- */
CREATE TABLE STOCK_HISTORY
(
`Date` DATE,
`Open` FLOAT,
High FLOAT,
Low FLOAT,
`Close` FLOAT,
Volume INT,
Dividends FLOAT,
Stock_Splits INT,
High_Low_Avg FLOAT,
Avg_Trading FLOAT
)

CREATE TABLE STOCK_ACTION
(
`Date` DATE,
Dividends FLOAT,
Stock_Splits INT
)


/* ---------- Loading the Dataset ---------- */
load data infile 'D:/Prot/Data Analyst/UM selected projects/A_Stock Data/TCS_stock_history.csv'
into table STOCK_HISTORY
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

load data infile 'D:/Prot/Data Analyst/UM selected projects/A_Stock Data/TCS_stock_action.csv'
into table STOCK_ACTION
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;


/* ---------- Querying the dataset after loading ---------- */
/* ---------- Querying the dataset after loading ---------- */
SELECT * FROM STOCK_HISTORY;
SELECT * FROM STOCK_ACTION;


/* ---------- Checking the 2 datasets if some other Stock Splits and Dividends were given on same day ---------- */
SELECT * FROM STOCK_HISTORY h, STOCK_ACTION a WHERE h.`Date` = a.`Date` AND h.Dividends != a.Dividends AND h.Stock_Splits != a.Stock_Splits;


-- No other Dividends and Stock splits were given on the same day. So analysing 'STOCK_ACTION' table is unnecessary.


/* ---------- Checking for highest ever closing price ---------- */
SELECT `Date`,`Close` FROM STOCK_HISTORY ORDER BY `Close` DESC;


/* ---------- Checking what is the highest price ever bid ---------- */
SELECT `Date`,High FROM STOCK_HISTORY ORDER BY High DESC;


/* ---------- Checking FY 2021-2022 ---------- */
SELECT * FROM STOCK_HISTORY WHERE YEAR(`Date`) = 2021 ORDER BY High DESC


/* ---------- Checking records where the closing price was above ₹3000 ---------- */
SELECT `Date`,`Close` FROM STOCK_HISTORY WHERE `Close` > 3000 ORDER BY `Close`;


/* ---------- Finding the highest closing price and its date ---------- */
SELECT * FROM (SELECT `Date`,`Close`,DENSE_RANK() OVER(ORDER BY `Close` DESC) AS Rank_no FROM STOCK_HISTORY) AS Ranked WHERE Rank_no =3;


/* ---------- Calculating average closing price for each year ---------- */
SELECT YEAR(`Date`) AS `Year`, ROUND(AVG(`Close`),2) AS Avg_closing_price FROM STOCK_HISTORY GROUP BY `Year` ORDER BY `Year`;


/* ---------- Calculating the total traded volume per month ---------- */
SELECT YEAR(`Date`) AS `Year`, MONTH(`Date`) AS `Month`,SUM(Volume) AS Total_volume_per_month FROM STOCK_HISTORY GROUP BY YEAR,MONTH ORDER BY YEAR,MONTH


/* ---------- Counting how many days stock closed above ₹2000 in each year ---------- */
SELECT YEAR(`Date`) AS `Year`, COUNT(*) AS Days_Above_3500 FROM STOCK_HISTORY WHERE `Close` > 2000 GROUP BY `Year` ORDER BY `Year`;


/* ---------- Finding percentage change in stock price from open to close for each day ---------- */
SELECT DAY(`Date`) AS Day,`Open`,`Close`, ROUND((`Open` - `Close`)*100/(`Open`),2) AS Daily_Return_Percent FROM STOCK_HISTORY ORDER BY Day


/* ---------- Calculating daily price difference ---------- */
SELECT DAY(`Date`) AS Day,ROUND((`Close` - `Open`),2) AS Daily_price_difference FROM STOCK_HISTORY  ORDER BY Day


/* ---------- Querying for top 5 highest volume trading days ---------- */
SELECT * FROM (SELECT `Date`, Volume, DENSE_RANK() OVER(ORDER BY Volume DESC) AS Highest_volume_rank FROM STOCK_HISTORY) AS VOL 
WHERE Highest_volume_rank >= 0 AND Highest_volume_rank <= 5 


/* ---------- Using a window function to find the moving average closing price (5-day) ---------- */
SELECT `Date`, `Close`, ROUND(AVG(`Close`) OVER(ORDER BY `Date` ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),2) AS Moving_Avg_5_Day
FROM STOCK_HISTORY


/* ---------- Finding the day when the stock had the maximum single-day gain ---------- */
SELECT `Date`, ROUND((`Close` - `Open`),2) AS Max_singleday_gain FROM STOCK_HISTORY ORDER BY Max_singleday_gain DESC


/* ---------- Calculating year-to-date return for each year (using first & last close price) ---------- */

WITH yearly_prices AS
(
SELECT YEAR(`Date`) AS YEAR,
FIRST_VALUE(`Close`) OVER(PARTITION BY YEAR(`Date`) ORDER BY `Date`) AS First_Close,
LAST_VALUE(`Close`) OVER(PARTITION BY YEAR(`Date`) ORDER BY `Date` ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS Last_Close
FROM STOCK_HISTORY
)
SELECT DISTINCT YEAR,ROUND(((Last_Close-First_Close)*100/First_Close),2) AS Yearly_Return_Percent
FROM yearly_prices;


/* ---------- Ranking months by average closing price ---------- */
SELECT YEAR(`Date`) AS `YEAR`,MONTH(`Date`) AS `Month`, ROUND(AVG(`Close`),2) AS Avg_closing_price, 
RANK() OVER(PARTITION BY YEAR(`Date`) ORDER BY AVG(`Close`) DESC) AS Month_Rank
FROM STOCK_HISTORY GROUP BY YEAR(`Date`), MONTH(`Date`);

/* ---------- Finding the best month for investing based on historical average return ---------- */
SELECT MONTH(`Date`) AS `Month`, ROUND(AVG((`Close` - `Open`)/`Open`)*100,2) AS Avg_Monthly_Daily_Return_Percent FROM STOCK_HISTORY
GROUP BY `Month` ORDER BY Avg_Monthly_Daily_Return_Percent DESC;


/* ---------- Calculating volatility (standard deviation of daily returns) for each year ---------- */
SELECT YEAR(`Date`) AS `Year`, ROUND(STDDEV_POP((`Close` - `Open`)/`Open`)*100,2) AS Volatility_Percent FROM STOCK_HISTORY
GROUP BY `Year` ORDER BY Volatility_Percent DESC;


/* ---------- Identifying days with gap-up openings (opening > previous day’s close) ---------- */
WITH prev_price AS (
SELECT DAY(`Date`) AS `Date`,`Open`,`Close`,
LAG(`Close`) OVER(ORDER BY `Date`) AS Prev_Close FROM STOCK_HISTORY
)
SELECT * FROM prev_price WHERE `Open` > Prev_Close


SELECT * FROM (SELECT DAY(`Date`) AS `Date`,`Open`,`Close`,
LAG(`Close`) OVER(ORDER BY `Date`) AS Prev_Close FROM STOCK_HISTORY) AS GAP
WHERE `Open` > Prev_Close


/* ---------- Find cumulative returns over time ---------- */

WITH daily_returns  AS (
SELECT `Date`,
LOG(`Close` / LAG(`Close`) OVER (ORDER BY `Date`)) AS log_return
FROM STOCK_HISTORY
)
SELECT `Date`, ROUND(EXP(SUM(log_return) OVER (ORDER BY `Date`)) -1,4)
AS Cumulative FROM daily_returns;


/* ---------- Find longest streak of consecutive gains ---------- */

WITH gains AS 
(
	SELECT `Date`,
    CASE WHEN `Close` > LAG(`Close`) OVER (ORDER BY `Date`) THEN 1 ELSE 0 END AS is_gain FROM STOCK_HISTORY
),
grouped AS 
(
    SELECT `Date`,is_gain,
	SUM(CASE WHEN is_gain = 0 THEN 1 ELSE 0 END) OVER (ORDER BY `Date`) AS grp
    FROM gains
)
SELECT COUNT(*) AS streak_length, MIN(`Date`) AS start_date, MAX(`Date`) AS end_date
FROM grouped WHERE is_gain = 1 GROUP BY grp
ORDER BY streak_length DESC
LIMIT 1;



