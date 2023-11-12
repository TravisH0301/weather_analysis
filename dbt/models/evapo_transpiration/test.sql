{{
    config(
        materialized='incremental'
    )
}}

select distinct
    station_name || '_' || to_varchar(date, 'yyyymmdd') as record_id,
    maximum_temperature as max_temp,
    minimum_temperature as min_temp,
    current_date() as load_date
from {{ source("staging", "weather_preprocessed") }} as source
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }}
    where (source.station_name || '_' || to_varchar(source.date, 'yyyymmdd')) = record_id
)
{% endif %}
