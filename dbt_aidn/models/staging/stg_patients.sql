{{ config(materialized='view') }}

with

raw_patients as (
    select * from {{ source('raw', 'patients') }}
)

select
    cast(patient_id          as varchar)     as patient_id,
    cast(name                as varchar)     as name,
    cast(primary_provider_id as varchar)     as primary_provider_id,
    cast(postcode            as varchar)     as postcode,
    cast(updated_at          as timestamptz) as updated_time,
    cast(_dlt_load_id        as varchar)     as _dlt_load_id,
    cast(_dlt_id             as varchar)     as _dlt_id
from raw_patients
