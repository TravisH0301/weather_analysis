{
    config(
        materialized='incremental'
    )
}

{ generate_temperature_model(2015) }