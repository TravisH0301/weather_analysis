{{
    config(
        materialized='incremental'
    )
}}

{{
    generate_year_partition_model(
        "MAXIMUM_TEMPERATURE, MINIMUM_TEMPERATURE, MAXIMUM_TEMPERATURE - MINIMUM_TEMPERATURE AS VARIANCE_TEMPERATURE, ", 2012
    )
}}