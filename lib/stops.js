'use strict'

// https://gtfs.org/schedule/reference/#stopstxt
const beforeAll = (opt) => `\
DO $$
BEGIN
  	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'location_type_val') THEN
    	CREATE TYPE "${opt.schema}".location_type_val AS ENUM (
      		'stop', 'station', 'entrance_exit', 'node', 'boarding_area'
    	);
    	CREATE CAST ("${opt.schema}".location_type_val AS text) WITH INOUT AS IMPLICIT;
  	END IF;

  	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'wheelchair_boarding_val') THEN
    	CREATE TYPE "${opt.schema}".wheelchair_boarding_val AS ENUM (
      		'no_info_or_inherit', 'accessible', 'not_accessible'
    	);
    	CREATE CAST ("${opt.schema}".wheelchair_boarding_val AS text) WITH INOUT AS IMPLICIT;
  	END IF;

  	-- Création d'une table temporaire
  	DROP TABLE IF EXISTS "${opt.schema}".temp_stops;
  	CREATE TABLE "${opt.schema}".temp_stops (LIKE "${opt.schema}".stops INCLUDING ALL);
END $$;

COPY "${opt.schema}".temp_stops (
	stop_id,
	stop_code,
	stop_name,
	stop_desc,
	stop_loc,
	zone_id,
	stop_url,
	location_type,
	parent_station,
	stop_timezone,
	wheelchair_boarding,
	level_id,
	platform_code
) FROM STDIN csv;
`;

const locationType = (val) => {
	if (val === '0') return 'stop'
	if (val === '1') return 'station'
	if (val === '2') return 'entrance_exit'
	if (val === '3') return 'node'
	if (val === '4') return 'boarding_area'
	throw new Error('invalid/unsupported location_type: ' + val)
}

const wheelchairBoarding = (val) => {
	if (val === '0') return 'no_info_or_inherit'
	if (val === '1') return 'accessible'
	if (val === '2') return 'not_accessible'
	throw new Error('invalid/unsupported wheelchair_boarding: ' + val)
}

const formatStopsRow = (s) => {
	return [
		s.stop_id || null,
		s.stop_code || null,
		s.stop_name || null,
		s.stop_desc || null,
		`POINT(${parseFloat(s.stop_lon)} ${parseFloat(s.stop_lat)})`,
		s.zone_id || null,
		s.stop_url || null,
		s.location_type
			? locationType(s.location_type)
			: null,
		s.parent_station || null,
		s.stop_timezone || null,
		s.wheelchair_boarding
			? wheelchairBoarding(s.wheelchair_boarding)
			: null,
		s.level_id || null,
		s.platform_code || null,
	]
}

const afterAll = (opt) => `\
\\.
DO $$
BEGIN
  -- Insérer les données depuis la table temporaire en gérant les conflits
  INSERT INTO "${opt.schema}".stops
  SELECT * FROM "${opt.schema}".temp_stops
  ON CONFLICT (stop_id) DO UPDATE SET
    stop_code = EXCLUDED.stop_code,
    stop_name = EXCLUDED.stop_name,
    stop_desc = EXCLUDED.stop_desc,
    stop_loc = EXCLUDED.stop_loc,
    zone_id = EXCLUDED.zone_id,
    stop_url = EXCLUDED.stop_url,
    location_type = EXCLUDED.location_type,
    parent_station = EXCLUDED.parent_station,
    stop_timezone = EXCLUDED.stop_timezone,
    wheelchair_boarding = EXCLUDED.wheelchair_boarding,
    level_id = EXCLUDED.level_id,
    platform_code = EXCLUDED.platform_code
  ;

  -- Nettoyer après insertion
  DROP TABLE IF EXISTS "${opt.schema}".temp_stops;

  -- Vérifier et ajouter la contrainte de clé étrangère si elle n'existe pas
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'stops_parent_station_fkey' AND connamespace = (SELECT oid FROM pg_namespace WHERE nspname = '${opt.schema}')
  ) THEN
    ALTER TABLE "${opt.schema}".stops ADD CONSTRAINT stops_parent_station_fkey FOREIGN KEY (parent_station) REFERENCES "${opt.schema}".stops;
  END IF;

  -- Créer des index s'ils n'existent pas
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'index_parent_station' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '${opt.schema}')) THEN
    CREATE INDEX index_parent_station ON "${opt.schema}".stops (parent_station);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'index_stop_loc' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '${opt.schema}')) THEN
    CREATE INDEX index_stop_loc ON "${opt.schema}".stops (stop_loc);
  END IF;
END $$;
`;

module.exports = {
	beforeAll,
	formatRow: formatStopsRow,
	afterAll,
};

