CREATE TABLE IF NOT EXISTS patients (
    patient_id   TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    birth_date   TEXT,
    gender       TEXT
);

CREATE TABLE IF NOT EXISTS encounters (
    encounter_id TEXT PRIMARY KEY,
    patient_id   TEXT REFERENCES patients(patient_id),
    status       TEXT,
    date         TEXT
);