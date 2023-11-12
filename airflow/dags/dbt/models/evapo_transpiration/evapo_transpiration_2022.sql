{{
    config(
        materialized='incremental'
    )
}}

{{
    generate_year_partition_model(
        "EVAPO_TRANSPIRATION, ", 2022
    )
}}