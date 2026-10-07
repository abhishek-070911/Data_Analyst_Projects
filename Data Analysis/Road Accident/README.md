# Road Accident Burden Across Indian Cities

MySQL, Python and Power BI analysis of road accidents in 50 Indian cities: accidents, deaths and injuries, broken down by cause.

**Data:** 9,550 rows. Each row is one city × cause category × cause subcategory × outcome (accidents, persons killed, grievously injured, minor injury, total injured).

**Data check:** the table repeats every accident across its 6 cause categories (traffic violation, weather, junction, road feature, traffic control, impacting vehicle). Each category sums to the same totals, so adding up all rows overstates totals 6x. All metrics are computed within one category (see `SQL File/Road_Accident_corrected.sql`).

## Findings

- 58,736 accidents and 13,542 deaths: 23.1 deaths per 100 accidents.
- Chennai had the most accidents (4,389); Delhi had the most deaths (1,196).
- Asansol Durgapur had the highest fatality rate: 73.6 deaths per 100 accidents.
- Over-speeding accounted for 66.5% of deaths.

## Files

| File | What's in it |
|---|---|
| `SQL File/Road_Accident_sql.sql` | Table setup, loading, cleaning and the exploratory queries |
| `SQL File/Road_Accident_corrected.sql` | The check that found the 6x repeat, and the findings above, each with its expected result |
| `Python File/Road_Accident_pyt.ipynb` | The same analysis in pandas, with charts |
| `Power BI File/Road_Accident_vis.pbix` | Dashboard |
| `Excel File/Road_Accident_exc.csv` | The dataset |

The Power BI dashboard still sums across all categories, so its totals are 6x too high. Use the figures above.
