{{
    config(
        materialized='incremental'
    )
}}

{{
    generate_year_partition_model_macro(
        "MAXIMUM_RELATIVE_HUMIDITY, MINIMUM_RELATIVE_HUMIDITY, ", 2015
    )
}}