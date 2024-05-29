with fhv_data as (
    select *,
    row_number() over (partition by dispatching_base_num, pickup_datetime order by dispatching_base_num) as rn
    from {{ source('staging','fhv_taxi_data') }}
    where dispatching_base_num is not null
)

select
    -- Indentifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num','pickup_datetime']) }} as tripid,
    dispatching_base_num::varchar as dispatching_base_num,
    pulocationid::int as pickup_locationid,
    DOLOCATIONID::int as dropoff_locationid,
    affiliated_base_number::varchar as affiliated_base_number,

    -- Timestamps
    pickup_datetime::timestamp as pickup_datetime,
    dropoff_datetime::timestamp as dropoff_datetime,


    -- flags
    sr_flag,
    {{ is_shared_ride("sr_flag") }} as sr_flag_description
from fhv_data
where rn = 1

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}