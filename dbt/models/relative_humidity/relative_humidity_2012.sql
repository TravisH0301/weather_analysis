{
    config(
        materialized='incremental'
    )
}

{ generate_relative_humidity_model(2012) }