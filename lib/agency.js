'use strict'

// https://gtfs.org/schedule/reference/#agencytxt
const beforeAll = (opt) => `\
CREATE TABLE IF NOT EXISTS "${opt.schema}".agency (
    agency_id TEXT PRIMARY KEY,
    agency_name TEXT NOT NULL,
    agency_url TEXT NOT NULL,
    agency_timezone TEXT NOT NULL CONSTRAINT valid_timezone CHECK ("${opt.schema}".is_timezone(agency_timezone)),
    agency_lang TEXT, -- todo: validate?
    agency_phone TEXT,
    agency_fare_url TEXT,
    agency_email TEXT
);

-- Création d'une table temporaire
DROP TABLE IF EXISTS "${opt.schema}".temp_agency;
CREATE TABLE "${opt.schema}".temp_agency (LIKE "${opt.schema}".agency INCLUDING ALL);

COPY "${opt.schema}".temp_agency (
    agency_id,
    agency_name,
    agency_url,
    agency_timezone,
    agency_lang,
    agency_phone,
    agency_fare_url,
    agency_email
) FROM STDIN csv;
`

const formatAgencyRow = (a) => {
	return [
		a.agency_id || null,
		a.agency_name || null,
		a.agency_url || null,
		a.agency_timezone || null,
		a.agency_lang || null,
		a.agency_phone || null,
		a.agency_fare_url || null,
		a.agency_email || null,
	]
}

const afterAll = (opt) => `\
\\.
-- Insérer les données depuis la table temporaire
INSERT INTO "${opt.schema}".agency
SELECT * FROM "${opt.schema}".temp_agency
ON CONFLICT (agency_id) DO UPDATE SET
    agency_name = EXCLUDED.agency_name,
    agency_url = EXCLUDED.agency_url,
    agency_timezone = EXCLUDED.agency_timezone,
    agency_lang = EXCLUDED.agency_lang,
    agency_phone = EXCLUDED.agency_phone,
    agency_fare_url = EXCLUDED.agency_fare_url,
    agency_email = EXCLUDED.agency_email
;

-- Nettoyer après insertion
DROP TABLE IF EXISTS "${opt.schema}".temp_agency;
`

module.exports = {
	beforeAll,
	formatRow: formatAgencyRow,
	afterAll,
}
