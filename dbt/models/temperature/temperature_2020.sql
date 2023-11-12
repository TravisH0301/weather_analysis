{{
    config(
        materialized='incremental'
    )
}}

{{
    generate_year_partition_model(
        "MAXIMUM_TEMPERATURE, MINIMUM_TEMPERATURE, "
    )
}}