/*
This query finds the weather station and the year-month time when it had the 
biggest monthly average temperature variance since 2012. 
Additionally, it pairs up the weather station's monthly average rain fall and
wind speed.

Solution Approach:
- Step 1: Three CTE tables are created with unioned tables of temperature, rain and
          wind speed from 2012.

- Step 2: Additional CTE is created by joining the union CTE tables by using
          the join key - record_id, which is a synthetic key consisted of 
          station name and date in a form of <station_name>_<yyyymmdd>.

- Step 3: Coupled with the above joining, the CTE is grouped by station name,
          year and month to calculate the monthly average values of the
          temperature variance, rain fall and wind speed.

- Step 4: In the outer query, the dataset is ordered by the monthly average
          temperature variance in descending order, and the row is limited to 1
          to display the weather station and the year-month time that had the
          biggested monthly average temperature variance, alongside monthly average
          of the rain fall and wind speed.

*****
Please note that the answer may be differ in year 2023 depending on the version of
BOM dataset as the dataset gets updated daily, creating difference in the record numbers.
*****
*/


with temp_union as (
    select record_id, station_name, date, variance_temperature from temperature.temperature_2023 
    union
    select record_id, station_name, date, variance_temperature from temperature.temperature_2022
    union
    select record_id, station_name, date, variance_temperature from temperature.temperature_2021
    union
    select record_id, station_name, date, variance_temperature from temperature.temperature_2020
    union
    select record_id, station_name, date, variance_temperature from temperature.temperature_2019
    union
    select record_id, station_name, date, variance_temperature from temperature.temperature_2018
    union
    select record_id, station_name, date, variance_temperature from temperature.temperature_2017
    union
    select record_id, station_name, date, variance_temperature from temperature.temperature_2016
    union
    select record_id, station_name, date, variance_temperature from temperature.temperature_2015
    union
    select record_id, station_name, date, variance_temperature from temperature.temperature_2014
    union
    select record_id, station_name, date, variance_temperature from temperature.temperature_2013
    union
    select record_id, station_name, date, variance_temperature from temperature.temperature_2012
),

rain_union as (
    select record_id, station_name, date, rain from rain.rain_2023 
    union
    select record_id, station_name, date, rain from rain.rain_2022
    union
    select record_id, station_name, date, rain from rain.rain_2021
    union
    select record_id, station_name, date, rain from rain.rain_2020
    union
    select record_id, station_name, date, rain from rain.rain_2019
    union
    select record_id, station_name, date, rain from rain.rain_2018
    union
    select record_id, station_name, date, rain from rain.rain_2017
    union
    select record_id, station_name, date, rain from rain.rain_2016
    union
    select record_id, station_name, date, rain from rain.rain_2015
    union
    select record_id, station_name, date, rain from rain.rain_2014
    union
    select record_id, station_name, date, rain from rain.rain_2013
    union
    select record_id, station_name, date, rain from rain.rain_2012
),

wind_speed_union as (
    select record_id, station_name, date, AVERAGE_10M_WIND_SPEED from wind_speed.wind_speed_2023 
    union
    select record_id, station_name, date, AVERAGE_10M_WIND_SPEED from wind_speed.wind_speed_2022
    union
    select record_id, station_name, date, AVERAGE_10M_WIND_SPEED from wind_speed.wind_speed_2021
    union
    select record_id, station_name, date, AVERAGE_10M_WIND_SPEED from wind_speed.wind_speed_2020
    union
    select record_id, station_name, date, AVERAGE_10M_WIND_SPEED from wind_speed.wind_speed_2019
    union
    select record_id, station_name, date, AVERAGE_10M_WIND_SPEED from wind_speed.wind_speed_2018
    union
    select record_id, station_name, date, AVERAGE_10M_WIND_SPEED from wind_speed.wind_speed_2017
    union
    select record_id, station_name, date, AVERAGE_10M_WIND_SPEED from wind_speed.wind_speed_2016
    union
    select record_id, station_name, date, AVERAGE_10M_WIND_SPEED from wind_speed.wind_speed_2015
    union
    select record_id, station_name, date, AVERAGE_10M_WIND_SPEED from wind_speed.wind_speed_2014
    union
    select record_id, station_name, date, AVERAGE_10M_WIND_SPEED from wind_speed.wind_speed_2013
    union
    select record_id, station_name, date, AVERAGE_10M_WIND_SPEED from wind_speed.wind_speed_2012
),

join_group as (
    select
        temp_union.station_name,
        extract(year from temp_union.date) as year,
        extract(month from temp_union.date) as month,
        avg(variance_temperature) as avg_var_temp,
        avg(rain) as avg_rain,
        avg(AVERAGE_10M_WIND_SPEED) as avg_10m_wind_speed
    from temp_union
    left join rain_union on temp_union.record_id = rain_union.record_id
    left join wind_speed_union on temp_union.record_id = wind_speed_union.record_id
    where variance_temperature is not null
    group by temp_union.station_name, year, month
)

select
    station_name,
    year,
    month,
    avg_var_temp,
    avg_rain,
    avg_10m_wind_speed
from join_group
order by avg_var_temp desc
limit 5;