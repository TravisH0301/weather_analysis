{{
    config(
        materialized='incremental'
    )
}}

{{
    generate_year_partition_model_macro(
        "RAIN, ", 2014
    )
}}