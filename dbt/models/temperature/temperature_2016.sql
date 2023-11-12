{{
    config(
        materialized='incremental'
    )
}}
{{
    generate_year_partition_model(
        "MAX_TEMPERATURE, MIN_TEMPERATURE, "
    )
}}