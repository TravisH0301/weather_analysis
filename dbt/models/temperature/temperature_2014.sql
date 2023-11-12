{{
    config(
        materialized='incremental'
    )
}}

{{
    generate_year_partition_model(
        "MAXIMUM_TEMPERATURE, MAXIMUM_TEMPERATURE, MAXIMUM_TEMPERATURE - MAXIMUM_TEMPERATURE AS VARIANCE_TEMPERATURE, ", 2014
    )
}}