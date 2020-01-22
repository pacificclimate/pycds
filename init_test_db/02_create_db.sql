-- Database

CREATE DATABASE crmp WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'en_CA.UTF-8' LC_CTYPE = 'en_CA.UTF-8';
ALTER DATABASE crmp OWNER TO crmp;

\connect crmp

-- Schema

CREATE SCHEMA crmp;
ALTER SCHEMA crmp OWNER TO crmp;

-- PL/pgSQL

-- CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
-- COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';

-- PL/Python

CREATE EXTENSION IF NOT EXISTS plpythonu WITH SCHEMA pg_catalog;;
COMMENT ON EXTENSION plpythonu IS 'PL/Python procedural language';

-- PostGIS

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;
COMMENT ON EXTENSION postgis IS 'PostGIS geometry, geography, and raster spatial types and functions';
