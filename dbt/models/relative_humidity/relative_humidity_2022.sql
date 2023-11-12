{{
    config(
        materialized='incremental'
    )
}}

{{
    generate_year_partition_model(
        "MAXIMUM_RELATIVE_HUMIDITY, MINIMUM_RELATIVE_HUMIDITY, ", 2022
    )
}}