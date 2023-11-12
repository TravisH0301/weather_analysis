{
    config(
        materialized='incremental'
    )
}

{ generate_solar_radiation_model(2019) }