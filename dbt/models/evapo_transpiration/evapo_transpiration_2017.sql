{{
    config(
        materialized='incremental'
    )
}}
{{
    generate_year_parition_model(
        "EVAPO_TRANSPIRATION, "
    )
}}