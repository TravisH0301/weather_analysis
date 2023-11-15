/*
This query finds the number of days that were above 35 degrees at 
Bunnings Notting Hill and Kmart Belmont in each of the last 9 full years.

The nearest weather stations from these stores were determined by
calculating the Euclidean distance between cooridnate points as 
illustrated in `find_nearest_weather_station.sql`.
This flat plane approach was used given the points are all in the same states.

**Solution Approach**:
- Step 1: This question is approached by using a CTE with unioned temperature
          year partition tables from 2014 to 2022 - note that 2023 is not a full year. 

- Step 2: Then in the outer query, the dataset is filtered by stations and 
          maximum temperatures according to the conditions provided by the question.

- Step 3: Finally, the dataset is grouped by station and year to count 
          the number of days that met with the above conditions.

- Step 4: The output dataset is ordered by store name and year to display results.
*/

WITH UNION_CTE AS (
    SELECT STATION_NAME, DATE, MAXIMUM_TEMPERATURE FROM TEMPERATURE.TEMPERATURE_2022
    UNION
    SELECT STATION_NAME, DATE, MAXIMUM_TEMPERATURE FROM TEMPERATURE.TEMPERATURE_2021
    UNION
    SELECT STATION_NAME, DATE, MAXIMUM_TEMPERATURE FROM TEMPERATURE.TEMPERATURE_2020
    UNION
    SELECT STATION_NAME, DATE, MAXIMUM_TEMPERATURE FROM TEMPERATURE.TEMPERATURE_2019
    UNION
    SELECT STATION_NAME, DATE, MAXIMUM_TEMPERATURE FROM TEMPERATURE.TEMPERATURE_2018
    UNION
    SELECT STATION_NAME, DATE, MAXIMUM_TEMPERATURE FROM TEMPERATURE.TEMPERATURE_2017
    UNION
    SELECT STATION_NAME, DATE, MAXIMUM_TEMPERATURE FROM TEMPERATURE.TEMPERATURE_2016
    UNION
    SELECT STATION_NAME, DATE, MAXIMUM_TEMPERATURE FROM TEMPERATURE.TEMPERATURE_2015
    UNION
    SELECT STATION_NAME, DATE, MAXIMUM_TEMPERATURE FROM TEMPERATURE.TEMPERATURE_2014
)

SELECT 
    CASE 
        WHEN STATION_NAME = 'MOORABBIN AIRPORT' THEN 'BUNNINGS NOTTING HILL'
        WHEN STATION_NAME = 'PERTH AIRPORT' THEN 'KMART BELMONT'
    END AS STORE_NAME,
    EXTRACT(YEAR FROM DATE) AS YEAR,
    COUNT(1) AS DAYS
FROM UNION_CTE
WHERE UPPER(STATION_NAME) IN ('MOORABBIN AIRPORT', 'PERTH AIRPORT')
    AND MAXIMUM_TEMPERATURE > 35
GROUP BY STATION_NAME, YEAR
ORDER BY STORE_NAME, YEAR DESC;
