{{
    config(
        materialized='incremental'
    )
}}

{{
    generate_year_partition_model(
        "MAX_RELATIVE_HUMIDITY, MIN_RELATIVE_HUMIDITY, "
    )
}}