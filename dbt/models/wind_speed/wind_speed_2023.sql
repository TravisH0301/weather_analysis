{{
    config(
        materialized='incremental'
    )
}}
{{
    generate_year_partition_model(
        "AVG_10M_WIND_SPEED, "
    )
}}