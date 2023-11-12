{{
    config(
        materialized='incremental'
    )
}}

{{
    generate_year_partition_model_macro(
        "MAXIMUM_TEMPERATURE, MINIMUM_TEMPERATURE, MAXIMUM_TEMPERATURE - MINIMUM_TEMPERATURE AS VARIANCE_TEMPERATURE, ", 2021
    )
}}