select

    -- identifiers
    cast(vendorid as int64)               as vendor_id,
    cast(pulocationid as int64)           as pickup_location_id,
    cast(dolocationid as int64)           as dropoff_location_id,

    -- timestamps
    cast(lpep_pickup_datetime as timestamp)  as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip information
    cast(passenger_count as float64)      as passenger_count,
    cast(trip_distance as float64)        as trip_distance,
    cast(ratecodeid as float64)           as rate_code_id,
    cast(store_and_fwd_flag as string)    as store_and_forward_flag,
    cast(trip_type as float64)            as trip_type,

    -- payment information
    cast(fare_amount as float64)          as fare_amount,
    cast(extra as float64)                as extra_charge,
    cast(mta_tax as float64)              as mta_tax,
    cast(tip_amount as float64)           as tip_amount,
    cast(tolls_amount as float64)         as tolls_amount,
    cast(ehail_fee as float64)            as ehail_fee,
    cast(improvement_surcharge as float64) as improvement_surcharge,
    cast(congestion_surcharge as float64)  as congestion_surcharge,
    cast(total_amount as float64)         as total_amount,
    cast(payment_type as float64)         as payment_type

 from {{source("raw_data", "green_tripdata_partitioned")}}

where vendorid is not null
