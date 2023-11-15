{{
    config(
        materialized='table'
    )
}}

with evapo_trans_union as (
    select record_id, station_name, state, date, evapo_transpiration from {{ ref('evapo_transpiration_2023') }} 
    union
    select record_id, station_name, state, date, evapo_transpiration from {{ ref('evapo_transpiration_2022') }}
    union
    select record_id, station_name, state, date, evapo_transpiration from {{ ref('evapo_transpiration_2021') }}
    union
    select record_id, station_name, state, date, evapo_transpiration from {{ ref('evapo_transpiration_2020') }}
    union
    select record_id, station_name, state, date, evapo_transpiration from {{ ref('evapo_transpiration_2019') }}
    union
    select record_id, station_name, state, date, evapo_transpiration from {{ ref('evapo_transpiration_2018') }}
    union
    select record_id, station_name, state, date, evapo_transpiration from {{ ref('evapo_transpiration_2017') }}
    union
    select record_id, station_name, state, date, evapo_transpiration from {{ ref('evapo_transpiration_2016') }}
    union
    select record_id, station_name, state, date, evapo_transpiration from {{ ref('evapo_transpiration_2015') }}
    union
    select record_id, station_name, state, date, evapo_transpiration from {{ ref('evapo_transpiration_2014') }}
    union
    select record_id, station_name, state, date, evapo_transpiration from {{ ref('evapo_transpiration_2013') }}
    union
    select record_id, station_name, state, date, evapo_transpiration from {{ ref('evapo_transpiration_2012') }}
),

rain_union as (
    select record_id, station_name, state, date, rain from {{ ref('rain_2023') }} 
    union
    select record_id, station_name, state, date, rain from {{ ref('rain_2022') }}
    union
    select record_id, station_name, state, date, rain from {{ ref('rain_2021') }}
    union
    select record_id, station_name, state, date, rain from {{ ref('rain_2020') }}
    union
    select record_id, station_name, state, date, rain from {{ ref('rain_2019') }}
    union
    select record_id, station_name, state, date, rain from {{ ref('rain_2018') }}
    union
    select record_id, station_name, state, date, rain from {{ ref('rain_2017') }}
    union
    select record_id, station_name, state, date, rain from {{ ref('rain_2016') }}
    union
    select record_id, station_name, state, date, rain from {{ ref('rain_2015') }}
    union
    select record_id, station_name, state, date, rain from {{ ref('rain_2014') }}
    union
    select record_id, station_name, state, date, rain from {{ ref('rain_2013') }}
    union
    select record_id, station_name, state, date, rain from {{ ref('rain_2012') }}
),

pan_evapo_union as (
    select record_id, station_name, state, date, pan_evaporation from {{ ref('pan_evaporation_2023') }} 
    union
    select record_id, station_name, state, date, pan_evaporation from {{ ref('pan_evaporation_2022') }}
    union
    select record_id, station_name, state, date, pan_evaporation from {{ ref('pan_evaporation_2021') }}
    union
    select record_id, station_name, state, date, pan_evaporation from {{ ref('pan_evaporation_2020') }}
    union
    select record_id, station_name, state, date, pan_evaporation from {{ ref('pan_evaporation_2019') }}
    union
    select record_id, station_name, state, date, pan_evaporation from {{ ref('pan_evaporation_2018') }}
    union
    select record_id, station_name, state, date, pan_evaporation from {{ ref('pan_evaporation_2017') }}
    union
    select record_id, station_name, state, date, pan_evaporation from {{ ref('pan_evaporation_2016') }}
    union
    select record_id, station_name, state, date, pan_evaporation from {{ ref('pan_evaporation_2015') }}
    union
    select record_id, station_name, state, date, pan_evaporation from {{ ref('pan_evaporation_2014') }}
    union
    select record_id, station_name, state, date, pan_evaporation from {{ ref('pan_evaporation_2013') }}
    union
    select record_id, station_name, state, date, pan_evaporation from {{ ref('pan_evaporation_2012') }}
),

temp_union as (
    select record_id, station_name, state, date, variance_temperature from {{ ref('temperature_2023') }} 
    union
    select record_id, station_name, state, date, variance_temperature from {{ ref('temperature_2022') }}
    union
    select record_id, station_name, state, date, variance_temperature from {{ ref('temperature_2021') }}
    union
    select record_id, station_name, state, date, variance_temperature from {{ ref('temperature_2020') }}
    union
    select record_id, station_name, state, date, variance_temperature from {{ ref('temperature_2019') }}
    union
    select record_id, station_name, state, date, variance_temperature from {{ ref('temperature_2018') }}
    union
    select record_id, station_name, state, date, variance_temperature from {{ ref('temperature_2017') }}
    union
    select record_id, station_name, state, date, variance_temperature from {{ ref('temperature_2016') }}
    union
    select record_id, station_name, state, date, variance_temperature from {{ ref('temperature_2015') }}
    union
    select record_id, station_name, state, date, variance_temperature from {{ ref('temperature_2014') }}
    union
    select record_id, station_name, state, date, variance_temperature from {{ ref('temperature_2013') }}
    union
    select record_id, station_name, state, date, variance_temperature from {{ ref('temperature_2012') }}
),

rel_hum_union as (
    select record_id, station_name, state, date, maximum_relative_humidity, minimum_relative_humidity from {{ ref('relative_humidity_2023') }} 
    union
    select record_id, station_name, state, date, maximum_relative_humidity, minimum_relative_humidity from {{ ref('relative_humidity_2022') }}
    union
    select record_id, station_name, state, date, maximum_relative_humidity, minimum_relative_humidity from {{ ref('relative_humidity_2021') }}
    union
    select record_id, station_name, state, date, maximum_relative_humidity, minimum_relative_humidity from {{ ref('relative_humidity_2020') }}
    union
    select record_id, station_name, state, date, maximum_relative_humidity, minimum_relative_humidity from {{ ref('relative_humidity_2019') }}
    union
    select record_id, station_name, state, date, maximum_relative_humidity, minimum_relative_humidity from {{ ref('relative_humidity_2018') }}
    union
    select record_id, station_name, state, date, maximum_relative_humidity, minimum_relative_humidity from {{ ref('relative_humidity_2017') }}
    union
    select record_id, station_name, state, date, maximum_relative_humidity, minimum_relative_humidity from {{ ref('relative_humidity_2016') }}
    union
    select record_id, station_name, state, date, maximum_relative_humidity, minimum_relative_humidity from {{ ref('relative_humidity_2015') }}
    union
    select record_id, station_name, state, date, maximum_relative_humidity, minimum_relative_humidity from {{ ref('relative_humidity_2014') }}
    union
    select record_id, station_name, state, date, maximum_relative_humidity, minimum_relative_humidity from {{ ref('relative_humidity_2013') }}
    union
    select record_id, station_name, state, date, maximum_relative_humidity, minimum_relative_humidity from {{ ref('relative_humidity_2012') }}
),

wind_speed_union as (
    select record_id, station_name, state, date, average_10m_wind_speed from {{ ref('wind_speed_2023') }} 
    union
    select record_id, station_name, state, date, average_10m_wind_speed from {{ ref('wind_speed_2022') }}
    union
    select record_id, station_name, state, date, average_10m_wind_speed from {{ ref('wind_speed_2021') }}
    union
    select record_id, station_name, state, date, average_10m_wind_speed from {{ ref('wind_speed_2020') }}
    union
    select record_id, station_name, state, date, average_10m_wind_speed from {{ ref('wind_speed_2019') }}
    union
    select record_id, station_name, state, date, average_10m_wind_speed from {{ ref('wind_speed_2018') }}
    union
    select record_id, station_name, state, date, average_10m_wind_speed from {{ ref('wind_speed_2017') }}
    union
    select record_id, station_name, state, date, average_10m_wind_speed from {{ ref('wind_speed_2016') }}
    union
    select record_id, station_name, state, date, average_10m_wind_speed from {{ ref('wind_speed_2015') }}
    union
    select record_id, station_name, state, date, average_10m_wind_speed from {{ ref('wind_speed_2014') }}
    union
    select record_id, station_name, state, date, average_10m_wind_speed from {{ ref('wind_speed_2013') }}
    union
    select record_id, station_name, state, date, average_10m_wind_speed from {{ ref('wind_speed_2012') }}
),

solar_rad_union as (
    select record_id, station_name, state, date, solar_radiation from {{ ref('solar_radiation_2023') }} 
    union
    select record_id, station_name, state, date, solar_radiation from {{ ref('solar_radiation_2022') }}
    union
    select record_id, station_name, state, date, solar_radiation from {{ ref('solar_radiation_2021') }}
    union
    select record_id, station_name, state, date, solar_radiation from {{ ref('solar_radiation_2020') }}
    union
    select record_id, station_name, state, date, solar_radiation from {{ ref('solar_radiation_2019') }}
    union
    select record_id, station_name, state, date, solar_radiation from {{ ref('solar_radiation_2018') }}
    union
    select record_id, station_name, state, date, solar_radiation from {{ ref('solar_radiation_2017') }}
    union
    select record_id, station_name, state, date, solar_radiation from {{ ref('solar_radiation_2016') }}
    union
    select record_id, station_name, state, date, solar_radiation from {{ ref('solar_radiation_2015') }}
    union
    select record_id, station_name, state, date, solar_radiation from {{ ref('solar_radiation_2014') }}
    union
    select record_id, station_name, state, date, solar_radiation from {{ ref('solar_radiation_2013') }}
    union
    select record_id, station_name, state, date, solar_radiation from {{ ref('solar_radiation_2012') }}
)

select distinct
    temp_union.station_name,
    extract(year from temp_union.date) as year,
    extract(month from temp_union.date) as month,
    avg(evapo_transpiration) as avg_evapo_transpiration,
    avg(rain) as avg_rain_fall,
    avg(pan_evaporation) as avg_pan_evaporation,
    avg(variance_temperature) as avg_var_temperature,
    avg(maximum_relative_humidity) as avg_max_rel_humidity,
    avg(minimum_relative_humidity) as avg_min_rel_humidity,
    avg(average_10m_wind_speed) as avg_10m_wind_speed,
    avg(solar_radiation) as avg_solar_radiation,
    temp_union.state,
    current_date() as load_date
from temp_union
left join evapo_trans_union on temp_union.record_id = evapo_trans_union.record_id
left join rain_union on temp_union.record_id = rain_union.record_id
left join pan_evapo_union on temp_union.record_id = pan_evapo_union.record_id
left join rel_hum_union on temp_union.record_id = rel_hum_union.record_id
left join wind_speed_union on temp_union.record_id = wind_speed_union.record_id
left join solar_rad_union on temp_union.record_id = solar_rad_union.record_id
where temp_union.state in ('VIC', 'WA')
group by temp_union.station_name, temp_union.state, year, month
