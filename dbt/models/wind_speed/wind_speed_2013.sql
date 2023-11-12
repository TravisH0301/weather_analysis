{
    config(
        materialized='incremental'
    )
}

{ generate_wind_speed_model(2013) }