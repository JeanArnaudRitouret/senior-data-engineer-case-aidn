{{ config(materialized='view') }}

with

raw_patient_consents as (
    select * from {{ source('raw', 'patient_consents') }}
)

select
    cast(patient_id            as varchar)     as patient_id,
    cast(consent_research      as boolean)     as consent_research,
    cast(consent_marketing     as boolean)     as consent_marketing,
    cast(consent_partner_share as boolean)     as consent_partner_share,
    cast(_dlt_valid_from       as timestamptz) as _dlt_valid_from,
    cast(_dlt_valid_to         as timestamptz) as _dlt_valid_to,
    cast(_dlt_load_id          as varchar)     as _dlt_load_id,
    cast(_dlt_id               as varchar)     as _dlt_id
from raw_patient_consents
