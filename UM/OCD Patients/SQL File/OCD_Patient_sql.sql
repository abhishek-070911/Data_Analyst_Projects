/* ---------- Creating  Database ---------- */
CREATE DATABASE OCD;
USE OCD;


/* ---------- Creating Table ---------- */
CREATE TABLE Patients
(
Patient_ID INT,
Age INT,
Gender ENUM('Male','Female'),
Ethnicity VARCHAR(50),
Marital_Status VARCHAR(50),
Education_Level VARCHAR(100),
OCD_Diagnosis_Date DATE,
Duration_of_Symptoms_months INT,
Previous_Diagnoses VARCHAR(50),
Family_History_of_OCD ENUM('Yes','No'),
Obsession_Type VARCHAR(100),
Compulsion_Type VARCHAR(100),
`Y-BOCS_Score_Obsessions` INT,
`Y-BOCS_Score_Compulsions` INT,
Depression_Diagnosis ENUM('Yes','No'),
Anxiety_Diagnosis ENUM('Yes','No'),
Medications VARCHAR(100)
);

/* ---------- Loading the Dataset ---------- */
load data infile 'D:/Prot/Data Analyst/UM selected projects/I_OCD Patients/OCD Patient Dataset_ Demographics & Clinical Data_cleaned.csv'
into table Patients
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;


/* ---------- Trimming extra spaces if present ---------- */
UPDATE Patients
SET
	Ethnicity = TRIM(Ethnicity),
    Marital_Status = TRIM(Marital_Status),
    Education_Level = TRIM(Education_Level),
    Previous_Diagnoses = TRIM(Previous_Diagnoses),
    Obsession_Type = TRIM(Obsession_Type),
    Compulsion_Type = TRIM(Compulsion_Type),
    Medications = TRIM(Medications);


/* ---------- Querying the dataset after loading ---------- */
/* ---------- Querying the dataset after loading ---------- */
SELECT * FROM Patients;


/* ---------- Listing unique values of Ethnicity ---------- */
SELECT DISTINCT Ethnicity FROM Patients;


/* ---------- Filtering patients with Age > 50 ---------- */
SELECT * FROM Patients WHERE Age >=50 ORDER BY Age DESC;


/* ---------- Order patients by OCD_Diagnosis_Date ---------- */
SELECT * FROM Patients ORDER BY OCD_Diagnosis_Date DESC;


/* ---------- Counting number of patients per Gender ---------- */
SELECT Gender,COUNT(*) AS Total_Patient FROM Patients GROUP BY Gender;


/* ---------- Finding average Age of patients ---------- */
SELECT ROUND(AVG(Age),2) AS Average_Age FROM Patients;


/* ---------- Getting patients with both Depression and Anxiety diagnosis ---------- */
SELECT * FROM Patients WHERE Depression_Diagnosis = 'Yes' AND Anxiety_Diagnosis = 'Yes';


/* ---------- Grouping patients by Ethnicity and counting them ---------- */
SELECT Ethnicity,COUNT(*) AS Total_Count FROM Patients GROUP BY Ethnicity ORDER BY Total_Count DESC;


/* ---------- Finding minimum and maximum Duration of Symptoms months ---------- */
SELECT MIN(Duration_of_Symptoms_months) FROM Patients;
SELECT MAX(Duration_of_Symptoms_months) FROM Patients;


/* ---------- Getting top 5 patients with highest Y-BOCS obsession scores ---------- */
SELECT * FROM Patients ORDER BY `Y-BOCS_Score_Obsessions` DESC LIMIT 5;


/* ---------- Finding how many patients are on SSRIs ---------- */
SELECT * FROM Patients WHERE Medications = 'SSRI';


/* ---------- Calculating average Y-BOCS scores for each Gender. ---------- */
SELECT Gender,ROUND(AVG(`Y-BOCS_Score_Obsessions`),2) AS Average_Obsessions_Score, ROUND(AVG(`Y-BOCS_Score_Compulsions`),2) AS Average_Compulsions_Score
FROM Patients GROUP BY Gender;


/* ---------- Finding Ethnicity with highest average Y-BOCS total score (Obsessions + Compulsions) ---------- */
SELECT Ethnicity,ROUND(AVG(`Y-BOCS_Score_Obsessions`),2) AS Average_Obsessions_Score, ROUND(AVG(`Y-BOCS_Score_Compulsions`),2) AS Average_Compulsions_Score,
ROUND(AVG(`Y-BOCS_Score_Obsessions`),2) + ROUND(AVG(`Y-BOCS_Score_Compulsions`),2) AS Total_Score 
FROM Patients GROUP BY Ethnicity ORDER BY Total_Score DESC;


/* ---------- Ranking patients by total Y-BOCS score using RANK() ---------- */
SELECT Patient_ID, ROUND(SUM(`Y-BOCS_Score_Obsessions`),2) + ROUND(SUM(`Y-BOCS_Score_Compulsions`),2) AS Total_Score,
 RANK() OVER(ORDER BY ROUND(SUM(`Y-BOCS_Score_Obsessions`),2) + ROUND(SUM(`Y-BOCS_Score_Compulsions`),2) DESC) `Rank` FROM Patients GROUP BY Patient_ID;


/* ---------- Calculating percentage of patients with Depression in each Education Level ---------- */
SELECT Education_Level,
ROUND(COUNT(CASE WHEN Depression_Diagnosis = 'Yes' THEN 1 END) * 100.0 / COUNT(*),2) AS Depression_Percentage
FROM Patients
GROUP BY Education_Level
ORDER BY Depression_Percentage DESC;


/* ---------- Finding top 3 highest scoring patients (Y-BOCS total) per Ethnicity ---------- */
SELECT * FROM 
(SELECT Patient_ID,Ethnicity, (`Y-BOCS_Score_Obsessions` + `Y-BOCS_Score_Compulsions`) AS `Y-BOCS_total`,
DENSE_RANK() OVER(PARTITION BY Ethnicity ORDER BY (`Y-BOCS_Score_Obsessions` + `Y-BOCS_Score_Compulsions`) DESC) Top_3_Rank
FROM Patients ) AS score 
WHERE Top_3_Rank IN(1,2,3) ORDER BY Ethnicity,Top_3_Rank;


/* ---------- Computing average Age per Medication type, then select only those above 40 ---------- */
WITH avg_age_medication AS(
SELECT Medications, ROUND(AVG(Age),2) AS Avg_Age FROM Patients GROUP BY Medications 
)
SELECT * FROM avg_age_medication WHERE Avg_Age > 47;