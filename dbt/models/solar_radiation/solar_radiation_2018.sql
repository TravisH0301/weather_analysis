{{
    config(
        materialized='incremental'
    )
}}
{{
    generate_year_partition_model(
        "SOLAR_RADIATION, "
    )
}}