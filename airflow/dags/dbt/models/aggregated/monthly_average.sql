{{
    config(
        materialized='view'
    )
}}

with temp_union as (
    select record_id, station_name, state, date, variance_temperature from temperature.temperature_2023 
    union
    select record_id, station_name, state, date, variance_temperature from temperature.temperature_2022
    union
    select record_id, station_name, state, date, variance_temperature from temperature.temperature_2021
    union
    select record_id, station_name, state, date, variance_temperature from temperature.temperature_2020
    union
    select record_id, station_name, state, date, variance_temperature from temperature.temperature_2019
    union
    select record_id, station_name, state, date, variance_temperature from temperature.temperature_2018
    union
    select record_id, station_name, state, date, variance_temperature from temperature.temperature_2017
    union
    select record_id, station_name, state, date, variance_temperature from temperature.temperature_2016
    union
    select record_id, station_name, state, date, variance_temperature from temperature.temperature_2015
    union
    select record_id, station_name, state, date, variance_temperature from temperature.temperature_2014
    union
    select record_id, station_name, state, date, variance_temperature from temperature.temperature_2013
    union
    select record_id, station_name, state, date, variance_temperature from temperature.temperature_2012
),

rain_union as (
    select record_id, station_name, state, date, rain from rain.rain_2023 
    union
    select record_id, station_name, state, date, rain from rain.rain_2022
    union
    select record_id, station_name, state, date, rain from rain.rain_2021
    union
    select record_id, station_name, state, date, rain from rain.rain_2020
    union
    select record_id, station_name, state, date, rain from rain.rain_2019
    union
    select record_id, station_name, state, date, rain from rain.rain_2018
    union
    select record_id, station_name, state, date, rain from rain.rain_2017
    union
    select record_id, station_name, state, date, rain from rain.rain_2016
    union
    select record_id, station_name, state, date, rain from rain.rain_2015
    union
    select record_id, station_name, state, date, rain from rain.rain_2014
    union
    select record_id, station_name, state, date, rain from rain.rain_2013
    union
    select record_id, station_name, state, date, rain from rain.rain_2012
),

wind_speed_union as (
    select record_id, station_name, state, date, average_10m_wind_speed from wind_speed.wind_speed_2023 
    union
    select record_id, station_name, state, date, average_10m_wind_speed from wind_speed.wind_speed_2022
    union
    select record_id, station_name, state, date, average_10m_wind_speed from wind_speed.wind_speed_2021
    union
    select record_id, station_name, state, date, average_10m_wind_speed from wind_speed.wind_speed_2020
    union
    select record_id, station_name, state, date, average_10m_wind_speed from wind_speed.wind_speed_2019
    union
    select record_id, station_name, state, date, average_10m_wind_speed from wind_speed.wind_speed_2018
    union
    select record_id, station_name, state, date, average_10m_wind_speed from wind_speed.wind_speed_2017
    union
    select record_id, station_name, state, date, average_10m_wind_speed from wind_speed.wind_speed_2016
    union
    select record_id, station_name, state, date, average_10m_wind_speed from wind_speed.wind_speed_2015
    union
    select record_id, station_name, state, date, average_10m_wind_speed from wind_speed.wind_speed_2014
    union
    select record_id, station_name, state, date, average_10m_wind_speed from wind_speed.wind_speed_2013
    union
    select record_id, station_name, state, date, average_10m_wind_speed from wind_speed.wind_speed_2012
),

join_group as (
    select
        temp_union.station_name,
        temp_union.state,
        extract(year from temp_union.date) as year,
        extract(month from temp_union.date) as month,
        avg(variance_temperature) as average_var_temperature,
        avg(rain) as average_rain,
        avg(average_10m_wind_speed) as average_10m_wind_speed
    from temp_union
    left join rain_union on temp_union.record_id = rain_union.record_id
    left join wind_speed_union on temp_union.record_id = wind_speed_union.record_id
    where variance_temperature is not null
        and temp_union.state in ('VIC', 'WA')
    group by temp_union.station_name, temp_union.state, year, month
)

select distinct
    station_name,
    state,
    year,
    month,
    average_var_temperature,
    average_rain,
    average_10m_wind_speed
from join_group
