'use strict'

const beforeAll = (opt) => `
DO $$
BEGIN
  	IF NOT EXISTS (
    	  SELECT 1 FROM pg_type WHERE typname = 'availability' AND typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '${opt.schema}')
  	) THEN
        CREATE TYPE "${opt.schema}".availability AS ENUM (
            'not_available', -- 0 – Service is not available for Mondays in the date range.
            'available' -- 1 – Service is available for all Mondays in the date range.
        );
  	END IF;

    CREATE TABLE IF NOT EXISTS "${opt.schema}".calendar (
        service_id TEXT PRIMARY KEY,
        monday "${opt.schema}".availability NOT NULL,
        tuesday "${opt.schema}".availability NOT NULL,
        wednesday "${opt.schema}".availability NOT NULL,
        thursday "${opt.schema}".availability NOT NULL,
        friday "${opt.schema}".availability NOT NULL,
        saturday "${opt.schema}".availability NOT NULL,
        sunday "${opt.schema}".availability NOT NULL,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL
    );

  	-- Création d'une table temporaire
  	DROP TABLE IF EXISTS "${opt.schema}".temp_calendar;
  	CREATE TABLE "${opt.schema}".temp_calendar (LIKE "${opt.schema}".calendar INCLUDING ALL);
END $$;

COPY "${opt.schema}".temp_calendar (
	service_id,
	monday,
	tuesday,
	wednesday,
	thursday,
	friday,
	saturday,
	sunday,
	start_date,
	end_date
) FROM STDIN csv;
`;

const availability = (val) => {
	if (val === '0') return 'not_available';
	if (val === '1') return 'available';
	throw new Error('Invalid availability: ' + val);
}

const formatCalendarRow = (c) => {
	return [
		c.service_id || null,
		c.monday ? availability(c.monday) : null,
		c.tuesday ? availability(c.tuesday) : null,
		c.wednesday ? availability(c.wednesday) : null,
		c.thursday ? availability(c.thursday) : null,
		c.friday ? availability(c.friday) : null,
		c.saturday ? availability(c.saturday) : null,
		c.sunday ? availability(c.sunday) : null,
		c.start_date,
		c.end_date,
	];
}

const afterAll = (opt) => `\\.
DO $$
BEGIN
    -- Insérer les données depuis la table temporaire en gérant les conflits
    INSERT INTO "${opt.schema}".calendar
    SELECT * FROM "${opt.schema}".temp_calendar
    ON CONFLICT (service_id) DO UPDATE SET
        monday = EXCLUDED.monday,
        tuesday = EXCLUDED.tuesday,
        wednesday = EXCLUDED.wednesday,
        thursday = EXCLUDED.thursday,
        friday = EXCLUDED.friday,
        saturday = EXCLUDED.saturday,
        sunday = EXCLUDED.sunday,
        start_date = EXCLUDED.start_date,
        end_date = EXCLUDED.end_date
    ;

    -- Nettoyer après insertion
    DROP TABLE IF EXISTS "${opt.schema}".temp_calendar;
END $$;
`

module.exports = {
	beforeAll,
	formatRow: formatCalendarRow,
	afterAll,
};
