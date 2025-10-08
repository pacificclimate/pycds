
-- =============================================================================
-- Test data for climatological tables migration v_7244176be9fa
-- =============================================================================

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

-- =============================================================================
-- CLIMATOLOGICAL PERIODS
-- =============================================================================

INSERT INTO climo_period (climo_period_id, start_date, end_date) 
VALUES (1, '2020-01-01', '2020-12-31');

SELECT setval('climo_period_climo_period_id_seq'::regclass, max(climo_period_id)) FROM climo_period;

-- =============================================================================
-- CLIMATOLOGICAL VARIABLES
-- =============================================================================

INSERT INTO climo_variable (climo_variable_id, duration, unit, standard_name, display_name, short_name, cell_methods, net_var_name)
VALUES 
    (1, 'annual', 'mm', 'snowfall_amount', 'Snowfall Amount', 'snowfall_amt', 'time: sum', 'snowfall_amt'),
    (2, 'monthly', 'degC', 'air_temperature', 'Air Temperature', 'air_temp', 'time: mean', 'air_temp'),
    (3, 'seasonal', 'mm/day', 'precipitation_flux', 'Precipitation Rate', 'precip_rate', 'time: mean', 'precip_rate');

SELECT setval('climo_variable_climo_variable_id_seq'::regclass, max(climo_variable_id)) FROM climo_variable;

-- =============================================================================
-- CLIMATOLOGICAL STATIONS
-- =============================================================================

INSERT INTO climo_station (climo_station_id, type, basin_id, comments, climo_period_id)
VALUES 
    (1, 'long-record', NULL, 'Station with long record', 1),
    (2, 'composite', 2002, 'Test composite station', 1),
    (3, 'prism', NULL, 'Test PRISM station', 1);

SELECT setval('climo_station_climo_station_id_seq'::regclass, max(climo_station_id)) FROM climo_station;

-- =============================================================================
-- CLIMATOLOGICAL STATION X HISTORY (Cross-reference table)
-- =============================================================================

INSERT INTO climo_stn_x_hist (climo_station_id, history_id, role)
VALUES 
    (1, 13216, 'base'),
    (2, 13216, 'base'),
    (3, 13216, 'joint');

-- =============================================================================
-- CLIMATOLOGICAL VALUES
-- =============================================================================

INSERT INTO climo_value (climo_value_id, value_time, value, num_contributing_years, climo_variable_id, climo_station_id)
VALUES 
    (1, '2020-01-15 00:00:00', 45.2, 20, 1, 1),
    (2, '2020-02-15 00:00:00', 15.8, 25, 2, 2),
    (3, '2020-03-15 00:00:00', 2.5, 30, 3, 3);

SELECT setval('climo_value_climo_value_id_seq'::regclass, max(climo_value_id)) FROM climo_value;