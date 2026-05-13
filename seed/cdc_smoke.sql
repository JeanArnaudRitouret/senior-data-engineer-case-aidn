INSERT INTO providers (provider_id, name, specialty)
VALUES ('SMOKE_PRV_INS', 'Dr Smoke Test', 'gp');

UPDATE providers
SET    specialty = 'cardiology'
WHERE  provider_id = (SELECT provider_id FROM providers
                      WHERE  provider_id NOT LIKE 'SMOKE_%'
                      ORDER BY provider_id LIMIT 1);

DELETE FROM providers
WHERE  provider_id = (SELECT provider_id FROM providers
                      WHERE  provider_id NOT LIKE 'SMOKE_%'
                      ORDER BY provider_id LIMIT 1 OFFSET 1);

-- UPDATE targets ingested_at so that dlt's dedup_sort=ingested_at desc picks the new row.

INSERT INTO appointments (event_id, appointment_id, patient_id, provider_id,
                          scheduled_at, status, event_timestamp, ingested_at)
VALUES ('SMOKE_APT_INS',
        'AP99999',
        (SELECT patient_id  FROM patients   WHERE patient_id  NOT LIKE 'SMOKE_%' ORDER BY patient_id  LIMIT 1),
        (SELECT provider_id FROM providers  WHERE provider_id NOT LIKE 'SMOKE_%' ORDER BY provider_id LIMIT 1),
        NOW(), 'scheduled', NOW(), NOW());

UPDATE appointments
SET    status      = 'completed',
       ingested_at = NOW()
WHERE  event_id = (SELECT event_id FROM appointments
                   WHERE  event_id NOT LIKE 'SMOKE_%'
                   ORDER BY event_id LIMIT 1);

DELETE FROM appointments
WHERE  event_id = (SELECT event_id FROM appointments
                   WHERE  event_id NOT LIKE 'SMOKE_%'
                   ORDER BY event_id LIMIT 1 OFFSET 1);

-- UPDATE bumps updated_at so the sql_table incremental cursor picks it up on next poll.
-- DELETE: sql_table polling is delete-blind by construction — the raw row must persist.

INSERT INTO patients (patient_id, name, primary_provider_id, postcode, updated_at)
VALUES ('SMOKE_PAT_INS', 'Smoke Patient', NULL, '9999', NOW());

UPDATE patients
SET    postcode   = '0000',
       updated_at = NOW()
WHERE  patient_id = (SELECT patient_id FROM patients
                     WHERE  patient_id NOT LIKE 'SMOKE_%'
                     ORDER BY patient_id LIMIT 1);

DELETE FROM patients
WHERE  patient_id = (SELECT patient_id FROM patients
                     WHERE  patient_id NOT LIKE 'SMOKE_%'
                     ORDER BY patient_id LIMIT 1 OFFSET 1);

-- UPDATE flips consent_research; the WAL event lands as a new row with lsn IS NOT NULL.
-- DELETE: the WAL DELETE event lands as a row with deleted_ts IS NOT NULL; prior rows are retained.

INSERT INTO patient_consents (patient_id, consent_research, consent_marketing, consent_partner_share)
VALUES ('SMOKE_CNS_INS', TRUE, FALSE, FALSE);

UPDATE patient_consents
SET    consent_research = NOT consent_research
WHERE  patient_id = (SELECT patient_id FROM patient_consents
                     WHERE  patient_id NOT LIKE 'SMOKE_%'
                     ORDER BY patient_id LIMIT 1);

DELETE FROM patient_consents
WHERE  patient_id = (SELECT patient_id FROM patient_consents
                     WHERE  patient_id NOT LIKE 'SMOKE_%'
                     ORDER BY patient_id LIMIT 1 OFFSET 1);
