/*
This query finds the nearest weather stations from Bunnings Notting Hill
and Kmart Belmont.
Note that Euclidean distance is used to determine the nearest weather stations.

**Approach**:
- Step 1: Using the coordinates of each stores, find the Euclidean distance to 
          all stations listed in the station dataset.
- Step 2: Union the results for each store.
- Step 3: Use the row_number() function and qualify clause to display the weather
          stations that have the minimum Euclidean distance to the stores.

- Bunnings Notting Hill coordinate: -37.900, 145.126 => MOORABBIN AIRPORT
- Kmart Belmont coordinate: -31.965, 115.935 => PERTH AIRPORT
*/

WITH DISTANCE_CTE AS (
    SELECT
        STATION_ID,
        'BUNNINGS NOTTING HILL' AS STORE_NAME,
        STATION_NAME AS NEAREST_WEATHER_STATION,
        LATITUDE,
        LONGITUDE,
        SQRT(
            POWER(LATITUDE - -37.900, 2) +
            POWER(LONGITUDE - 145.126, 2)
        ) AS DISTANCE
    FROM
        STAGING.STATION_PREPROCESSED
    UNION
    SELECT
        STATION_ID,
        'KMART BELMONT' AS STORE_NAME,
        STATION_NAME AS NEAREST_WEATHER_STATION,
        LATITUDE,
        LONGITUDE,
        SQRT(
            POWER(LATITUDE - -31.965, 2) +
            POWER(LONGITUDE - 115.935, 2)
        ) AS DISTANCE
    FROM
        STAGING.STATION_PREPROCESSED
)

SELECT
    STATION_ID,
    STORE_NAME,
    NEAREST_WEATHER_STATION,
    ROUND(DISTANCE, 2) AS EUCLIDEAN_DISTANCE
FROM DISTANCE_CTE
QUALIFY ROW_NUMBER() OVER (PARTITION BY STORE_NAME ORDER BY DISTANCE) = 1
