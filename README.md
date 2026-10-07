# Data Analyst Projects

SQL, Python and Power BI projects. Every project folder has the same layout:

- `Excel File`: the dataset (CSV)
- `SQL File`: the MySQL script, covering table design, loading, cleaning and the analysis queries
- `Python File`: a Jupyter notebook with the same analysis in pandas, plus charts
- `Power BI File`: the dashboard (.pbix)

## Data Analysis

Seven end-to-end analysis projects on public datasets.

| Project | What it covers |
|---|---|
| [Road Accident](Data%20Analysis/Road%20Accident) | Accidents, deaths and fatality rates in 50 Indian cities (9,550 rows). The dataset repeats every accident across 6 cause categories, so all totals are computed within one. Chennai had the most accidents and Delhi the most deaths. |
| [Stock Analysis](Data%20Analysis/Stock%20Analysis) | TCS daily prices, 2002–2021 (4,438 rows): yearly returns with FIRST_VALUE and LAST_VALUE, volatility, moving averages, and the longest winning streak with LAG. Rows from before TCS was listed in August 2004 are left out of long-run figures. |
| [OCD Patients](Data%20Analysis/OCD%20Patients) | 1,500 patient records: Y-BOCS severity by gender and ethnicity, the top patients in each ethnicity with DENSE_RANK, and depression rates by education level. |
| [Electric Vehicle](Data%20Analysis/Electric%20Vehicle) | EV sales in India by state, month and vehicle class, 2014–2024 (96,845 rows): year-over-year growth, running totals, state rankings, a monthly pivot and a stored procedure. |
| [Employee Salary](Data%20Analysis/Employee_Salary) | 44,289 employee pay records, 2011–2018: top earners in each job title, pay quartiles with NTILE, and comparisons with the job-title average. |
| [Laptop Prices](Data%20Analysis/Laptop%20Price%20Prediction) | 1,000 laptops from 19 brands: price by brand, CPU and operating system, price per GB of storage, and laptops priced above their brand's average. |
| [Summer Olympics](Data%20Analysis/Summer%20Olympics) | 15,316 medals from 1976 to 2008: top countries, the most successful athletes, the gold-medal leader at each Games, and sports where women won more medals than men. |

## Google Data Analytics

| Project | What it covers |
|---|---|
| [Cyclistic bike-share](Google%20Data%20Analytics/Cyclistic%20Dataset) | Capstone project for the Google Data Analytics Professional Certificate: how annual members and casual riders use the bikes differently. The dataset is linked in `Excel Files/Datasets From drive`. |

## Data engineering

My main project is [olist-data-platform](https://github.com/abhishek-070911/olist-data-platform), a data platform on about 100,000 real e-commerce orders. It is in progress: the source data is being profiled first, and incremental loads through Bronze, Silver and Gold layers come next.
