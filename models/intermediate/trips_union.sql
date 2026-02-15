with green_tripdata_partitioned as (
    select * from {{ref('stg_green_tripdata_partitioned')}} 
),

 yellow_tripdata_partitioned as (
    select * from {{ref('stg_yellow_tripdata_partitioned')}} 
)

trips_unioned as (
    select * from green_tripdata_partitioned
    union all
    select * from yellow_tripdata_partitioned
)