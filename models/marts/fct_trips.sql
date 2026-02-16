{{ config(materialized='table') }}

with trips_unioned as (
    select * from {{ ref('int_trips_unioned') }} -- Your intermediate model
),

dim_payment_type as (
    select * from {{ ref('payment_type') }} -- Your seed file
),

deduplicated_trips as (
    select 
        *,
        -- Generate a unique ID based on the natural keys of the trip
        {{ dbt_utils.generate_surrogate_key(['vendorid', 'lpep_pickup_datetime', 'lpep_dropoff_datetime']) }} as trip_id,
        row_number() over (
            partition by vendorid, lpep_pickup_datetime, lpep_dropoff_datetime 
            order by pickup_locationid -- Arbitrary tie-breaker
        ) as rn
    from trips_unioned
)

select
    -- Primary Key
    trip_id,
    
    -- Trip Info
    t.vendorid,
    t.service_type,
    t.ratecodeid,
    t.pickup_locationid,
    t.dropoff_locationid,
    t.lpep_pickup_datetime,
    t.lpep_dropoff_datetime,
    
    -- Payment Enrichment
    t.payment_type,
    p.description as payment_type_description,
    
    -- Metrics
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount

from deduplicated_trips t
left join dim_payment_type p 
    on t.payment_type = p.payment_type
where t.rn = 1 -- This removes the duplicates