{{ config(materialized='view') }}

-- CDC event log — one row per WAL event (INSERT / UPDATE / DELETE).

with

raw_patient_consents as (
    select * from {{ source('raw', 'patient_consents') }}
)

select
    cast(patient_id            as varchar)     as patient_id,
    cast(consent_research      as boolean)     as consent_research,
    cast(consent_marketing     as boolean)     as consent_marketing,
    cast(consent_partner_share as boolean)     as consent_partner_share,
    cast(lsn                   as bigint)      as lsn,
    cast(deleted_ts            as timestamptz) as deleted_ts,
    cast(_dlt_load_id          as varchar)     as _dlt_load_id
from raw_patient_consents
