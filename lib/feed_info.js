'use strict'

// https://gtfs.org/schedule/reference/#feed_infotxt
const beforeAll = (opt) => `\
-- The MobilityData GTFS Validator just uses Java's Locale#toLanguageTag() to validate *_lang.
-- https://github.com/MobilityData/gtfs-validator/blob/31ff374800f7d7883fd9de91b71049c2a4de4e45/main/src/main/java/org/mobilitydata/gtfsvalidator/validator/MatchingFeedAndAgencyLangValidator.java#L82
-- https://docs.oracle.com/javase/7/docs/api/java/util/Locale.html
-- related: https://github.com/google/transit/pull/98

CREATE TABLE IF NOT EXISTS "${opt.schema}".feed_info (
    feed_publisher_name TEXT PRIMARY KEY,
    feed_publisher_url TEXT NOT NULL,
    feed_lang TEXT NOT NULL
        CONSTRAINT valid_feed_lang CHECK ("${opt.schema}".is_valid_lang_code(feed_lang)),
    default_lang TEXT
        CONSTRAINT valid_default_lang CHECK ("${opt.schema}".is_valid_lang_code(feed_lang)),
    feed_start_date DATE,
    feed_end_date DATE,
    feed_version TEXT,
    feed_contact_email TEXT,
    feed_contact_url TEXT
);

-- Création d'une table temporaire
DROP TABLE IF EXISTS "${opt.schema}".temp_feed_info;
CREATE TABLE "${opt.schema}".temp_feed_info (LIKE "${opt.schema}".feed_info INCLUDING ALL);

COPY "${opt.schema}".temp_feed_info (
    feed_publisher_name,
    feed_publisher_url,
    feed_lang,
    default_lang,
    feed_start_date,
    feed_end_date,
    feed_version,
    feed_contact_email,
    feed_contact_url
) FROM STDIN csv;
`

const formatFeedInfoRow = (i) => {
	return [
		i.feed_publisher_name || null,
		i.feed_publisher_url || null,
		i.feed_lang || null,
		i.default_lang || null,
		i.feed_start_date || null,
		i.feed_end_date || null,
		i.feed_version || null,
		i.feed_contact_email || null,
		i.feed_contact_url || null,
	]
}

const afterAll = (opt) => `\
\\.
-- Insérer les données depuis la table temporaire
INSERT INTO "${opt.schema}".feed_info
SELECT * FROM "${opt.schema}".temp_feed_info
ON CONFLICT (feed_publisher_name) DO UPDATE SET
    feed_publisher_url = EXCLUDED.feed_publisher_url,
    feed_lang = EXCLUDED.feed_lang,
    default_lang = EXCLUDED.default_lang,
    feed_start_date = EXCLUDED.feed_start_date,
    feed_end_date = EXCLUDED.feed_end_date,
    feed_version = EXCLUDED.feed_version,
    feed_contact_email = EXCLUDED.feed_contact_email,
    feed_contact_url = EXCLUDED.feed_contact_url
;

-- Nettoyer après insertion
DROP TABLE IF EXISTS "${opt.schema}".temp_feed_info;
`

module.exports = {
	beforeAll,
	formatRow: formatFeedInfoRow,
	afterAll,
}
