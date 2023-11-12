{{
    config(
        materialized='incremental'
    )
}}
{{
    generate_year_parition_model(
        "MAX_TEMPERATURE, MIN_TEMPERATURE, "
    )
}}