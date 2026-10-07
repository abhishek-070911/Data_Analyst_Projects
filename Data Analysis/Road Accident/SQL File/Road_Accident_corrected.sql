/* =====================================================================
   Road Accident Burden Across Indian Cities - corrected city metrics
   ---------------------------------------------------------------------
   Why this file exists:
   The Accidents table holds 6 parallel breakdowns of the SAME accidents,
   one per Cause_Category (Traffic Violation, Weather, Junction,
   Road Features, Traffic Control, Impacting Vehicle/Object).
   Every category sums to the same totals, so SUM(Count_in_mil) over all
   rows counts each accident 6 times. The outcome rows also mix units
   (accidents vs people injured or killed), and 'Total Injured' already
   includes the two injury rows.
   Rule: compute every total inside ONE Cause_Category, one outcome at a time.
   ===================================================================== */

USE Road_Accidents;

/* ---------- 1. Proof: all 6 categories give identical national totals ---------- */
SELECT Cause_Category, Outcome_of_Incident, SUM(Count_in_mil) AS total
FROM Accidents
WHERE Outcome_of_Incident IN ('Total number of Accidents', 'Persons Killed')
GROUP BY Cause_Category, Outcome_of_Incident
ORDER BY Outcome_of_Incident, Cause_Category;
-- Expected: 58,736 accidents in every category, and 13,542 deaths in every category except
-- Road Features, which shows 13,543. Three Coimbatore death counts in that category are fractions
-- in the source file (3.5, 15.75 and 1.75), and the INT column rounds each one up.


/* ---------- 2. Correct national totals (one category only) ---------- */
SELECT
    SUM(CASE WHEN Outcome_of_Incident = 'Total number of Accidents' THEN Count_in_mil ELSE 0 END) AS accidents,
    SUM(CASE WHEN Outcome_of_Incident = 'Persons Killed' THEN Count_in_mil ELSE 0 END) AS deaths,
    ROUND(100.0 * SUM(CASE WHEN Outcome_of_Incident = 'Persons Killed' THEN Count_in_mil ELSE 0 END)
                / SUM(CASE WHEN Outcome_of_Incident = 'Total number of Accidents' THEN Count_in_mil ELSE 0 END), 1)
        AS deaths_per_100_accidents
FROM Accidents
WHERE Cause_Category = 'Traffic Violation';
-- Expected: 58,736 accidents, 13,542 deaths, 23.1 deaths per 100 accidents.


/* ---------- 3. City metrics: volume, shares, fatality rate and both ranks ---------- */
WITH city AS (
    SELECT City_Name,
           SUM(CASE WHEN Outcome_of_Incident = 'Total number of Accidents' THEN Count_in_mil ELSE 0 END) AS accidents,
           SUM(CASE WHEN Outcome_of_Incident = 'Persons Killed' THEN Count_in_mil ELSE 0 END) AS deaths
    FROM Accidents
    WHERE Cause_Category = 'Traffic Violation'
    GROUP BY City_Name
)
SELECT City_Name, accidents, deaths,
       ROUND(100.0 * accidents / SUM(accidents) OVER (), 1) AS pct_of_accidents,
       ROUND(100.0 * deaths / SUM(deaths) OVER (), 1) AS pct_of_deaths,
       ROUND(100.0 * deaths / NULLIF(accidents, 0), 1) AS deaths_per_100_accidents,
       DENSE_RANK() OVER (ORDER BY accidents DESC) AS accident_rank,
       DENSE_RANK() OVER (ORDER BY deaths DESC) AS death_rank
FROM city
ORDER BY accident_rank;
-- Expected: Chennai #1 by accidents (4,389), Delhi #1 by deaths (1,196),
-- Asansol Durgapur the highest fatality rate (73.6 deaths per 100 accidents).


/* ---------- 4. Deaths by traffic violation ---------- */
-- Note: the source label 'Over' is over-speeding (the text after the hyphen was lost in the source file).
SELECT Cause_Subcategory, SUM(Count_in_mil) AS deaths,
       ROUND(100.0 * SUM(Count_in_mil) / SUM(SUM(Count_in_mil)) OVER (), 1) AS pct_of_deaths
FROM Accidents
WHERE Cause_Category = 'Traffic Violation' AND Outcome_of_Incident = 'Persons Killed'
GROUP BY Cause_Subcategory
ORDER BY deaths DESC;
-- Expected: 'Over' (over-speeding) = 9,003 deaths, 66.5%.
