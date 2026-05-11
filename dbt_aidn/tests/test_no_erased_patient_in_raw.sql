-- Singular test: fails if any erased patient (erased_at IS NOT NULL) still has
-- rows in any raw table. Returns one row per offending patient_id + table.
-- Zero rows = pass; any rows = purge_erased_patients did not complete cleanly.

with erased as (
    select patient_id
    from {{ ref('erasure_requests') }}
    where erased_at is not null
),

violations as (
    select 'raw.patients' as source_table, p.patient_id
    from {{ source('raw', 'patients') }} p
    inner join erased e on p.patient_id = e.patient_id

    union all

    select 'raw.appointments' as source_table, a.patient_id
    from {{ source('raw', 'appointments') }} a
    inner join erased e on a.patient_id = e.patient_id

    union all

    select 'raw.patient_consents' as source_table, pc.patient_id
    from {{ source('raw', 'patient_consents') }} pc
    inner join erased e on pc.patient_id = e.patient_id
)

select *
from violations
