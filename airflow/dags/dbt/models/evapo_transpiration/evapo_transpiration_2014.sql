{{
    config(
        materialized='incremental'
    )
}}

{{
    generate_year_partition_model_macro(
        "EVAPO_TRANSPIRATION, ", 2014
    )
}}