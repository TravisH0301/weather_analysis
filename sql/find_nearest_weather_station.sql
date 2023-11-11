/*
Query to find the nearest weather stations from Bunnings Notting Hill
and Kmart Belmont.
Note that Euclidean distance is calculated given points are within the
same state.

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
    ROUND(DISTANCE, 2) AS DISTANCE
FROM DISTANCE_CTE
QUALIFY ROW_NUMBER() OVER (PARTITION BY STORE_NAME ORDER BY DISTANCE) = 1
