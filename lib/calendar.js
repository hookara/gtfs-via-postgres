'use strict'

// https://gtfs.org/schedule/reference/#calendartxt
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
END $$;

DO $$
BEGIN
	IF NOT EXISTS (
		SELECT 1
    	FROM pg_tables
    	WHERE schemaname = '${opt.schema}' AND tablename = 'calendar'
  	) THEN
    	CREATE TABLE "${opt.schema}".calendar (
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
  	END IF;
END $$;

COPY "${opt.schema}".calendar (
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

const afterAll = `\
\\.
`

module.exports = {
	beforeAll,
	formatRow: formatCalendarRow,
	afterAll,
};
