/*
This query finds the number of days that were above 35 degrees at 
Bunnings Notting Hill and Kmart Belmont in each of the last 9 years.

The nearest weather stations from these stores were determined by
calculating the Euclidean distance between cooridnate points as 
illustrated in `find_nearest_weather_station.sql`.
This flat plane approach was used given the points are all in the same states.

Solution Approach:
- Step 1: This question is approached by using a CTE with unioned temperature
          year partition tables from 2014. 
- Step 2: Then in the outer query, the dataset is filtered by stations and 
          maximum temperatures according to the conditions provided by the question.
- Step 3: Finally, the dataset is grouped by station and year to count 
          the number of days that met with the above conditions.
- Step 4: The output dataset is ordered by store name and year to display results.
*/

with union_cte as (
    select station_name, date, maximum_temperature from temperature_2023 
    union
    select station_name, date, maximum_temperature from temperature_2022
    union
    select station_name, date, maximum_temperature from temperature_2021
    union
    select station_name, date, maximum_temperature from temperature_2020
    union
    select station_name, date, maximum_temperature from temperature_2019
    union
    select station_name, date, maximum_temperature from temperature_2018
    union
    select station_name, date, maximum_temperature from temperature_2017
    union
    select station_name, date, maximum_temperature from temperature_2016
    union
    select station_name, date, maximum_temperature from temperature_2015
    union
    select station_name, date, maximum_temperature from temperature_2014
)

select 
    case 
        when station_name = 'MOORABBIN AIRPORT' then 'Bunnings Notting Hill'
        when station_name = 'PERTH AIRPORT' then 'Kmart Belmont'
    end as store_name,
    extract(year from date) as year,
    count(1) as days
from union_cte
where upper(station_name) in ('MOORABBIN AIRPORT', 'PERTH AIRPORT')
    and maximum_temperature > 35
group by station_name, year
order by store_name, year desc;
