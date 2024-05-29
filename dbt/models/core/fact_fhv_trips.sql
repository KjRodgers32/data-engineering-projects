{{
    config(
        materialized='table'
    )
}}

with fhv_data as (
    select *
    from {{ ref('stg_fhv_taxi_data') }}
),

dim_zones as (
    select *
    from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    f.tripid,
    f.dispatching_base_num,
    f.affiliated_base_number,
    f.pickup_locationid,
    pd.borough as pickup_borough,
    pd.zone as pickup_zone,
    f.dropoff_locationid,
    dd.borough as dropoff_borough,
    dd.zone as dropoff_zone,
    f.pickup_datetime,
    f.dropoff_datetime,
    sr_flag,
    sr_flag_description
from fhv_data as f
join dim_zones as pd on
f.pickup_locationid = pd.locationid
join dim_zones as dd on
f.dropoff_locationid = dd.locationid

{% if var(is_test_run, default=True) %}

limit 100

{% endif %}