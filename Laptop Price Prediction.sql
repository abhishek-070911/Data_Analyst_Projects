/* ---------- Creating  Database ---------- */
CREATE DATABASE PROJECTS_UM
USE PROJECTS_UM


/* ---------- Creating Table ---------- */
CREATE TABLE LAPTOPS
(
Company VARCHAR(30),
Product VARCHAR(100),
Type_Name VARCHAR(30),
Inches FLOAT,
Ram_gb INT,
OS VARCHAR(30),
Weight_kg FLOAT,
Price_euros FLOAT,
Screen VARCHAR(30),
Screen_W_pixels int,
Screen_H_pixels int,
Touchscreen VARCHAR(10),
IPS_panel VARCHAR(10),
Retina_Display VARCHAR(10),
CPU_company VARCHAR(30),
CPU_freq_Hz FLOAT,
CPU_model VARCHAR(30),
Primary_Storage_gb INT,
Secondary_Storage_gb INT,
Primary_Storage_Type VARCHAR(30),
Secondary_Storage_Type VARCHAR(30),
GPU_company VARCHAR(30),
GPU_model VARCHAR(100)
)


/* ---------- Loading the Dataset ---------- */
load data infile 'D:/Prot/Data Analyst/UM selected projects/A_Laptop Price/laptop_prices_dataset.csv'
into table LAPTOPS
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;


/* ---------- Cleaning the dataset ---------- */
UPDATE LAPTOPS
SET
  Company = TRIM(Company),
  Product = TRIM(Product),
  Type_Name = TRIM(Type_Name),
  OS = TRIM(OS),
  Touchscreen = TRIM(Touchscreen),
  IPS_panel = TRIM(IPS_panel),
  Retina_Display = TRIM(Retina_Display),
  CPU_company = TRIM(CPU_company),
  CPU_model = TRIM(CPU_model),
  Primary_Storage_Type = TRIM(Primary_Storage_Type),
  Secondary_Storage_Type = TRIM(Secondary_Storage_Type),
  GPU_company = TRIM(GPU_company),
  GPU_model = TRIM(GPU_model);


/* ---------- Converting 'No' in Storage Type to NULL ---------- */
UPDATE LAPTOPS
SET Secondary_Storage_Type = NULL WHERE LOWER(Secondary_Storage_Type) = 'no'


/* ---------- Replace 0 in Storage Columns with NULL ---------- */
UPDATE LAPTOPS
SET Secondary_Storage_gb = NULL WHERE Secondary_Storage_gb = 0





/* ---------- Querying the dataset after loading ---------- */
/* ---------- Querying the dataset after loading ---------- */
SELECT * FROM LAPTOPS;


/* ---------- Querying for average price of laptop ---------- */
SELECT ROUND(AVG(Price_euros),2) AS Average_Price_of_Laptop FROM LAPTOPS;


/* ---------- Laptop count per company ---------- */
SELECT Company,COUNT(*) AS Total_count_of_laptops FROM LAPTOPS GROUP BY Company ORDER BY Total_count_of_laptops DESC;


/* ---------- Average RAM per company ---------- */
SELECT OS,ROUND(AVG(Ram_gb),2) AS Average_ram_per_company FROM LAPTOPS GROUP BY OS ORDER BY Average_ram_per_company DESC;


/* ---------- Querying RAM > 16 ---------- */
SELECT Company,Ram_gb FROM LAPTOPS WHERE Ram_gb > 16 AND Primary_Storage_Type = 'SSD' GROUP BY Company,Ram_gb ORDER BY Company;
SELECT * FROM LAPTOPS WHERE Ram_gb > 16 AND Primary_Storage_Type = 'SSD' ORDER BY Ram_gb DESC;


/* ---------- Querying laptops with Retina Display and Touchscreen ---------- */
SELECT * FROM LAPTOPS WHERE Touchscreen = 'Yes' and Retina_Display = 'Yes';
SELECT * FROM LAPTOPS WHERE Touchscreen = 'Yes' and Retina_Display = 'No';
SELECT * FROM LAPTOPS WHERE Touchscreen = 'No' and Retina_Display = 'Yes';
SELECT * FROM LAPTOPS WHERE Touchscreen = 'No' and Retina_Display = 'No';


/* ---------- Querying for laptops with Retina Display and Touchscreen ---------- */
SELECT Product , (Screen_W_pixels * Screen_H_pixels) AS Screen_Resolution FROM LAPTOPS GROUP BY Product,Screen_Resolution;


/* ---------- Querying for laptops with total storage ---------- */
SELECT Product, (Primary_Storage_gb + Secondary_Storage_gb) AS Total_storage_gb FROM LAPTOPS GROUP BY Product,Total_storage_gb
ORDER BY Total_storage_gb DESC;


/* ---------- Querying for TYPE OF laptops ---------- */
SELECT Type_Name,COUNT(*) AS Total_count FROM LAPTOPS GROUP BY Type_Name ORDER BY Total_count DESC;


/* ---------- Querying for Average price by CPU_company ---------- */
SELECT CPU_company,ROUND(AVG(Price_euros),2) AS Average_price FROM LAPTOPS GROUP BY CPU_company ORDER BY Average_price DESC;


/* ---------- Querying for Top 5 most expensive laptops ---------- */
SELECT Company,Product,Type_Name,Ram_gb,Inches,Screen,Weight_kg,GPU_model,Price_euros FROM LAPTOPS ORDER BY Price_euros DESC LIMIT 5;


/* ---------- Querying for Distribution of operating systems ---------- */
SELECT OS,COUNT(*) AS OS_total_count FROM LAPTOPS GROUP BY OS ORDER BY OS_total_count DESC;

/* ---------- Querying for average prices of each Type_Name across different OS ---------- */
SELECT OS,
	ROUND(AVG(CASE WHEN Type_Name = 'Notebook' THEN Price_euros END)) AS Average_notebook_price,
    ROUND(AVG(CASE WHEN Type_Name = 'Gaming' THEN Price_euros END)) AS Average_Gaming_price,
    ROUND(AVG(CASE WHEN Type_Name = 'Ultrabook' THEN Price_euros END)) AS Average_Ultrabook_price,
    ROUND(AVG(CASE WHEN Type_Name = '2 in 1 Convertible' THEN Price_euros END)) AS Average_Convertible_price
FROM LAPTOPS GROUP BY OS ORDER BY OS;


/* ---------- Querying for laptops priced above the average price of their brand ---------- */
SELECT * FROM LAPTOPS L1 WHERE Price_euros > (SELECT ROUND(AVG(Price_euros),2) FROM LAPTOPS L2 WHERE L2.Company = L1.Company)
ORDER BY Company,Price_euros DESC


/* ---------- Querying for the top 5 most common CPU-GPU pairs in the dataset ---------- */
SELECT CPU_model,GPU_model,COUNT(*) AS count FROM LAPTOPS GROUP BY 1,2 ORDER BY count DESC LIMIT 5;


/* ---------- Querying Price per GB combining primary and secondary storage ---------- */
SELECT Company,Product,(Primary_Storage_gb + Secondary_Storage_gb) AS Total_storage_gb,
ROUND(Price_euros/ NULLIF((Primary_Storage_gb + Secondary_Storage_gb),0),2) AS Price_per_gb FROM LAPTOPS WHERE
(Primary_Storage_gb + Secondary_Storage_gb) > 0 ORDER BY Price_per_gb DESC;


/* ---------- Querying to Estimate pixels per inch using resolution and screen size ---------- */

SELECT Company,Product,Screen_W_pixels, Screen_H_pixels, Inches,
ROUND(SQRT(POW(Screen_W_pixels,2) + POW(Screen_H_pixels,2))/Inches,2) as Pixels_per_inch FROM LAPTOPS
ORDER BY Pixels_per_inch DESC LIMIT 10;


/* ---------- Querying for average weight per laptop type and sorting by lowest ---------- */
SELECT Company,Product,Type_Name,ROUND(AVG(Weight_kg),2) AS Average_weight FROM LAPTOPS GROUP BY Company,Product,Type_Name
ORDER BY Average_weight DESC

SELECT Type_Name,ROUND(AVG(Weight_kg),2) AS Average_weight FROM LAPTOPS GROUP BY Type_Name
ORDER BY Average_weight DESC


/* ---------- Comparing for average prices of laptops with different Primary_Storage_Type ---------- */
SELECT Primary_Storage_Type,COUNT(*) AS Total_count,ROUND(AVG(Price_euros),2) AS Average_price FROM LAPTOPS
GROUP BY Primary_Storage_Type ORDER BY Average_price DESC


/* ---------- Querying for high end laptop ---------- */
SELECT Product, Company, Price_euros, Ram_gb, Retina_Display, IPS_panel, Primary_Storage_Type FROM LAPTOPS 
	WHERE Price_euros > 1500 
	AND Ram_gb >16
    AND (Retina_Display = 'Yes' OR IPS_Panel = 'Yes')
    AND Primary_Storage_Type = 'SSD'



SELECT * FROM LAPTOPS