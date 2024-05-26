'use strict'

// https://gtfs.org/schedule/reference/#calendar_datestxt
const beforeAll = (opt) => `
DO $$
BEGIN
  	IF NOT EXISTS (
		    SELECT 1 FROM pg_type WHERE typname = 'exception_type_v' AND typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '${opt.schema}')
  	) THEN
        CREATE TYPE "${opt.schema}".exception_type_v AS ENUM (
            'added', -- 1 – Service has been added for the specified date.
            'removed' -- 2 – Service has been removed for the specified date.
        );
  	END IF;

  	IF NOT EXISTS (
    	  SELECT 1 FROM pg_cast WHERE castsource = (SELECT oid FROM pg_type WHERE typname = 'exception_type_v') AND casttarget = (SELECT oid FROM pg_type WHERE typname = 'text')
  	) THEN
    	  CREATE CAST ("${opt.schema}".exception_type_v AS text) WITH INOUT AS IMPLICIT;
  	END IF;

    CREATE TABLE IF NOT EXISTS "${opt.schema}".calendar_dates (
        service_id TEXT NOT NULL,
        "date" DATE NOT NULL,
        PRIMARY KEY (service_id, "date"),
        exception_type "${opt.schema}".exception_type_v NOT NULL
    );

    -- Création d'une table temporaire
    DROP TABLE IF EXISTS "${opt.schema}".temp_calendar_dates;
    CREATE TABLE "${opt.schema}".temp_calendar_dates (LIKE "${opt.schema}".calendar_dates INCLUDING ALL);
END $$;

COPY "${opt.schema}".temp_calendar_dates (
    service_id,
    date,
    exception_type
) FROM STDIN csv;
`;

const exceptionType = (val) => {
	if (val === '1') return 'added'
	if (val === '2') return 'removed'

	throw new Error('invalid exception_type: ' + val)
}

const formatCalendarDatesRow = (e) => {
	return [
		e.service_id || null,
		e.date,
		e.exception_type ? exceptionType(e.exception_type) : null,
	]
}

const afterAll = (opt) => `\\.
DO $$
BEGIN
    CREATE INDEX IF NOT EXISTS idx_service_id ON "${opt.schema}".calendar_dates (service_id);
    CREATE INDEX IF NOT EXISTS idx_exception_type ON "${opt.schema}".calendar_dates (exception_type);

    ${opt.postgraphile ? `COMMENT ON TABLE "${opt.schema}".calendar_dates IS E'@foreignKey (service_id) references calendar|@fieldName calendar';` : ''}

    -- Insérer les données depuis la table temporaire
    INSERT INTO "${opt.schema}".calendar_dates
    SELECT * FROM "${opt.schema}".temp_calendar_dates
    ON CONFLICT (service_id, date) DO UPDATE SET
        exception_type = EXCLUDED.exception_type
    ;

    -- Nettoyer après insertion
    DROP TABLE IF EXISTS "${opt.schema}".temp_calendar_dates;
END $$;
`

module.exports = {
	beforeAll,
	formatRow: formatCalendarDatesRow,
	afterAll,
}
