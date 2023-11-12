{{
    config(
        materialized='incremental'
    )
}}

{{
    generate_year_partition_model(
        "PAN_EVAPORATION, ", 2013
    )
}}