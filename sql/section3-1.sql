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
    station_name,
    extract(year from date) as year,
    count(1) as days
from union_cte
where upper(station_name) in ('MOORABBIN AIRPORT', 'PERTH AIRPORT')
    and maximum_temperature > 35
group by station_name, year
order by store_name, year desc;
