{{
    config(
        materialized='incremental'
    )
}}
{{
    generate_year_parition_model(
        "RAIN, "
    )
}}