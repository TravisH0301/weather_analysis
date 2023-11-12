{{
    config(
        materialized='incremental'
    )
}}
{{
    generate_year_parition_model(
        "SOLAR_RADIATION, "
    )
}}