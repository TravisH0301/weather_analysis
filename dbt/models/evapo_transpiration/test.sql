{{
    config(
        materialized='incremental'
    )
}}

{{
    generate_temperature_model(
        "maximum_temperature as max_temperature, minimum_temperature as min_temperature,"
    ) 
}}
