{{
    config(
        materialized='incremental'
    )
}}

{{
    generate_year_partition_model_macro(
        "PAN_EVAPORATION, ", 2016
    )
}}