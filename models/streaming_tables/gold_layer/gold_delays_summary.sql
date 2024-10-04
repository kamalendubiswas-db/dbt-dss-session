{{
    config(
        materialized='streaming_table',
        tblproperties={"delta.enableChangeDataFeed":"true"},
        zorder="airline_name",
        tags='streaming'
    )
}}

    SELECT 
        airline_name
        ,ArrDate
        ,COUNT(*) AS no_flights
        ,SUM(IF(IsArrDelayed = TRUE,1,0)) AS tot_delayed
        ,ROUND(tot_delayed*100/no_flights,2) AS perc_delayed
        FROM STREAM({{ ref('airline_trips_silver') }})
        WHERE airline_name IS NOT NULL
        GROUP BY 1,2