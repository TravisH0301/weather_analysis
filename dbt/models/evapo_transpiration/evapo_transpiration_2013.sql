{
    config(
        materialized='incremental'
    )
}

{ generate_evapo_transpiration_model(2013) }