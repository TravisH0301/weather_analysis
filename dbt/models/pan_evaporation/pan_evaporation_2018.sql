{{
    config(
        materialized='incremental'
    )
}}
{{
    generate_year_parition_model(
        "PAN_EVAPORATION, "
    )
}}