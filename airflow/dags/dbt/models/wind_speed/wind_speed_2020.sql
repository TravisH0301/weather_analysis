{{
    config(
        materialized='incremental'
    )
}}

{{
    generate_year_partition_model(
        "AVERAGE_10M_WIND_SPEED, ", 2020
    )
}}