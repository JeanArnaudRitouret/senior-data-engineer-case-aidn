{{ config(materialized='view') }}

with

raw_providers as (
    select * from {{ source('raw', 'providers') }}
)

select
    cast(provider_id  as varchar)     as provider_id,
    cast(name         as varchar)     as name,
    cast(specialty    as varchar)     as specialty,
    cast(lsn          as bigint)      as lsn,
    cast(deleted_ts   as timestamptz) as deleted_time,
    cast(_dlt_load_id as varchar)     as _dlt_load_id,
    cast(_dlt_id      as varchar)     as _dlt_id
from raw_providers
