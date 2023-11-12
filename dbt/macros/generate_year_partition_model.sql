/*
This macro generates a year partition data model by receiving required 
attributes for the table. E.g., RAIN for RAIN_YYYY table.
*/

{% macro generate_year_partition_model(attributes) %}

select distinct
    station_name || '_' || to_varchar(date, 'yyyymmdd') as record_id,
    station_name,
    date,
    {{ attributes }}
    state,
    current_date() as load_date
from {{ source("staging", "weather_preprocessed") }} as source
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }}
    where (source.station_name || '_' || to_varchar(source.date, 'yyyymmdd')) = record_id
)
{% endif %}

{% endmacro %}