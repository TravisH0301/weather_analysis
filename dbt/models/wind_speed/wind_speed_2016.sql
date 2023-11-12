{
    config(
        materialized='incremental'
    )
}
{
    generate_year_parition_model(
        "AVG_10M_WIND_SPEED, "
    )
}