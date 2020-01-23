database=$1
owner=$2
schema=$3

cat <<EOT
-- Schema $schema

\connect $database

CREATE SCHEMA $schema;
ALTER SCHEMA $schema OWNER TO $owner;

-- PL/pgSQL

-- CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
-- COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';

-- PL/Python

CREATE EXTENSION IF NOT EXISTS plpythonu WITH SCHEMA pg_catalog;;
COMMENT ON EXTENSION plpythonu IS 'PL/Python procedural language';

-- PostGIS

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;
COMMENT ON EXTENSION postgis IS 'PostGIS geometry, geography, and raster spatial types and functions';

EOT
