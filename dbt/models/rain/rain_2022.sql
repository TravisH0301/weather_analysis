{
    config(
        materialized='incremental'
    )
}

{ generate_rain_model(2022) }