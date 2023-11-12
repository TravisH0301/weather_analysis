{
    config(
        materialized='incremental'
    )
}

{ generate_pan_evaporation_model(2017) }