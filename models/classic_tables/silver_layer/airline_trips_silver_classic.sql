{{
    config(
        materialized='table',
        tags='classic'
        )
}}

WITH 

origin_airport_codes as (

    SELECT
        iata_code
        ,municipality origin_city
        ,name as origin_airport_name
        ,elevation_ft::INT origin_elevation_ft
        ,split(coordinates,',') as origin_coordinates_array
    FROM {{source("bronze_layer", "airport_codes_bronze_classic")}}

),

dest_airport_codes as (

    SELECT
        iata_code
        ,municipality dest_city
        ,name as dest_airport_name
        ,elevation_ft::INT dest_elevation_ft
        ,split(coordinates,',') as dest_coordinates_array
    FROM {{source("bronze_layer", "airport_codes_bronze_classic")}}

),

airline_names as (

    SELECT iata, name as airline_name FROM {{source("bronze_layer", "airline_codes_bronze_classic")}}
),

bronze_raw as (

    SELECT 
        *
        ,TO_DATE(STRING(INT(Year*10000+Month*100+DayofMonth)),'yyyyMMdd') AS ArrDate
        ,TO_TIMESTAMP(STRING(BIGINT(Year*100000000+Month*1000000+DayofMonth*10000+ArrTime)),'yyyyMMddHHmm') AS ArrTimestamp
    FROM ({{source("bronze_layer", "airline_trips_bronze_classic")}})
),

final as (

SELECT 
  {{ dbt_utils.generate_surrogate_key([
                'ArrTimestamp'
            ])
        }} as delay_id
  ,ActualElapsedTime
  ,ArrDelay::INT
  ,CRSArrTime 
  ,CRSDepTime 
  ,CRSElapsedTime 
  ,Cancelled::INT
  ,ArrDate
  ,ArrTimestamp
  ,DayOfWeek
  ,DayOfMonth
  ,Month
  ,Year
  ,DepDelay::INT
  ,DepTime 
  ,Dest 
  ,Distance 
  ,Diverted::INT
  ,FlightNum 
  ,IsArrDelayed 
  ,IsDepDelayed
  ,Origin 
  ,UniqueCarrier
  ,airline_name
  ,origin_city
  ,origin_airport_name
  ,origin_elevation_ft
  ,origin_coordinates_array
  ,dest_city
  ,dest_airport_name
  ,dest_elevation_ft
  ,dest_coordinates_array
FROM bronze_raw raw
INNER JOIN origin_airport_codes
  ON raw.Origin = origin_airport_codes.iata_code
INNER JOIN dest_airport_codes
  ON raw.Dest = dest_airport_codes.iata_code  
INNER JOIN airline_names 
  ON raw.UniqueCarrier = airline_names.iata
)

SELECT * FROM final