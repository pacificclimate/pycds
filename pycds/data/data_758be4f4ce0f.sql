
SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;


INSERT INTO climo_period (climo_period_id, start_date, end_date) 
VALUES (1, '2020-01-01', '2020-12-31');
SELECT setval('climo_period_climo_period_id_seq'::regclass, max(climo_period_id)) FROM climo_period;

INSERT INTO climo_variable (climo_variable_id, duration, unit, standard_name, display_name, short_name, cell_methods, net_var_name)
VALUES (1, 'annual', 'mm', 'snowfall_amount', 'Snowfall Amount', 'snowfall_amt', 'time: sum', 'snowfall_amt');
SELECT setval('climo_variable_climo_variable_id_seq'::regclass, max(climo_variable_id)) FROM climo_variable;

INSERT INTO climo_station (climo_station_id, type, basin_id, comments, climo_period_id)
VALUES (1, 'long-record', NULL, 'Station with long record', 1);
SELECT setval('climo_station_climo_station_id_seq'::regclass, max(climo_station_id)) FROM climo_station;

INSERT INTO climo_stn_x_hist (climo_station_id, history_id, role)
VALUES (1, 13216, 'base');