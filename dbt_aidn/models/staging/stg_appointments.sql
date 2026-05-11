{{ config(materialized='view') }}

with

raw_appointments as (
    select * from {{ source('raw', 'appointments') }}
)

select
    cast(event_id        as varchar)     as event_id,
    cast(appointment_id  as varchar)     as appointment_id,
    cast(patient_id      as varchar)     as patient_id,
    cast(provider_id     as varchar)     as provider_id,
    cast(scheduled_at    as timestamptz) as scheduled_time,
    cast(status          as varchar)     as status,
    cast(event_timestamp as timestamptz) as event_time,
    cast(ingested_at     as timestamptz) as ingested_time,
    cast(lsn             as bigint)      as lsn,
    cast(deleted_ts      as timestamptz) as deleted_time,
    cast(_dlt_load_id    as varchar)     as _dlt_load_id,
    cast(_dlt_id         as varchar)     as _dlt_id
from raw_appointments
