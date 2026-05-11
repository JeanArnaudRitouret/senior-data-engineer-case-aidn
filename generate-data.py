import csv, random, uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

random.seed(42)
SEED = Path("seed"); SEED.mkdir(exist_ok=True)
now = datetime.now(timezone.utc)

providers = [
    (f"PR{i:03d}", f"Provider {i}", s)
    for i, s in enumerate(["GP", "Cardiology", "Pediatrics", "Dermatology", "GP", "Oncology"])
]

patients = []
for i in range(50):
    pid = f"PT{i:04d}"
    patients.append({
        "patient_id": pid,
        "name": f"Patient {i}",
        "primary_provider_id": random.choice(providers)[0],
        "postcode": f"{random.randint(1000, 9999)}",
        "updated_at": (now - timedelta(days=random.randint(30, 365))).isoformat(),
    })
    if random.random() < 0.3:
        patients.append({
            **patients[-1],
            "primary_provider_id": random.choice(providers)[0],
            "postcode": f"{random.randint(1000, 9999)}",
            "updated_at": (now - timedelta(days=random.randint(1, 29))).isoformat(),
        })

appointments = []
for i in range(500):
    apt_id = f"AP{i:05d}"
    scheduled = now - timedelta(days=random.randint(0, 90))
    appointments.append({
        "event_id": str(uuid.uuid4()),
        "appointment_id": apt_id,
        "patient_id": f"PT{random.randint(0, 49):04d}",
        "provider_id": random.choice(providers)[0],
        "scheduled_at": scheduled.isoformat(),
        "status": "scheduled",
        "event_timestamp": scheduled.isoformat(),
        "ingested_at": scheduled.isoformat(),
    })
    if random.random() < 0.6:
        event_ts = scheduled + timedelta(hours=random.randint(1, 48))
        ingested = event_ts + timedelta(hours=random.randint(0, 72))
        appointments.append({
            **appointments[-1],
            "event_id": str(uuid.uuid4()),
            "status": random.choice(["completed", "cancelled", "no_show"]),
            "event_timestamp": event_ts.isoformat(),
            "ingested_at": ingested.isoformat(),
        })
    if random.random() < 0.05:
        appointments.append(appointments[-1].copy())

consents = [{
    "patient_id": f"PT{i:04d}",
    "consent_research": random.random() < 0.6,
    "consent_marketing": random.random() < 0.3,
    "consent_partner_share": random.random() < 0.4,
} for i in range(50)]

def write_csv(name, rows, fields):
    with open(SEED / f"{name}.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
        for r in rows: w.writerow(r)

write_csv("providers", [{"provider_id": p[0], "name": p[1], "specialty": p[2]} for p in providers],
          ["provider_id", "name", "specialty"])
write_csv("patients", patients,
          ["patient_id", "name", "primary_provider_id", "postcode", "updated_at"])
write_csv("appointments", appointments,
          ["event_id", "appointment_id", "patient_id", "provider_id",
           "scheduled_at", "status", "event_timestamp", "ingested_at"])
write_csv("patient_consents", consents,
          ["patient_id", "consent_research", "consent_marketing", "consent_partner_share"])

(SEED / "init.sql").write_text("""
CREATE TABLE providers (provider_id TEXT PRIMARY KEY, name TEXT, specialty TEXT);
CREATE TABLE patients (patient_id TEXT, name TEXT, primary_provider_id TEXT, postcode TEXT, updated_at TIMESTAMPTZ);
-- event_id is not a PK: source emits duplicates (at-least-once delivery).
CREATE TABLE appointments (event_id TEXT, appointment_id TEXT, patient_id TEXT, provider_id TEXT,
                           scheduled_at TIMESTAMPTZ, status TEXT, event_timestamp TIMESTAMPTZ, ingested_at TIMESTAMPTZ);
CREATE TABLE patient_consents (patient_id TEXT PRIMARY KEY, consent_research BOOLEAN,
                               consent_marketing BOOLEAN, consent_partner_share BOOLEAN);
COPY providers        FROM '/seed/providers.csv'        CSV HEADER;
COPY patients         FROM '/seed/patients.csv'         CSV HEADER;
COPY appointments     FROM '/seed/appointments.csv'     CSV HEADER;
COPY patient_consents FROM '/seed/patient_consents.csv' CSV HEADER;

-- CDC infrastructure (logical replication)
ALTER TABLE appointments REPLICA IDENTITY FULL;
-- REPLICA IDENTITY FULL required on providers: WAL DELETE with DEFAULT only sends the PK;
-- non-nullable fields in the Pydantic model (name, specialty) cause ValidationError and
-- the DELETE row is dropped before reaching raw.providers. FULL sends the entire old row.
ALTER TABLE providers REPLICA IDENTITY FULL;
CREATE PUBLICATION aidn_providers_pub FOR TABLE providers;
CREATE PUBLICATION aidn_appointments_pub FOR TABLE appointments;
""")
print("seed/ generated")