{{
    config(
        materialized='table',
        tags='streaming'
    )
}}

select * 
from read_files('{{var("input_path")}}/iata_data/airline_codes.json')
