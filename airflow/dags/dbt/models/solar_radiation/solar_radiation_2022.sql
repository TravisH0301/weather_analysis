{{
    config(
        materialized='incremental'
    )
}}

{{
    generate_year_partition_model_macro(
        "SOLAR_RADIATION, ", 2022
    )
}}