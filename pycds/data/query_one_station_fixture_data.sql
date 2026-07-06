-- Minimal fixture for ORM query_one_station tests.
--
-- The target station/history and its temperature observations are copied from
-- crmp_subset_data.sql. Precipitation and wind speed are actual variables in
-- the same network, but are observed only at a synthetic peer station.

INSERT INTO meta_network (
    network_id,
    network_name,
    description,
    virtual,
    publish,
    col_hex,
    contact_id
) VALUES (
    12,
    'FLNRO-WMB',
    'BC Ministry of Forests, Lands, and Natural Resource Operations - Wild Fire Management Branch',
    NULL,
    true,
    '#0C6600',
    NULL
);

INSERT INTO meta_station (
    station_id,
    network_id,
    native_id,
    min_obs_time,
    max_obs_time,
    publish
) VALUES
    (2313, 12, '932', '2006-02-10 15:00:00', '2006-02-12 12:00:00', true),
    (2314, 12, 'query-one-station-peer', '2006-02-10 15:00:00', '2006-02-10 18:00:00', true),
    (2315, 12, 'query-one-station-empty', '2006-02-10 15:00:00', '2006-02-10 15:00:00', true);

INSERT INTO meta_history (
    history_id,
    station_id,
    station_name,
    lon,
    lat,
    elev,
    sdate,
    edate,
    tz_offset,
    province,
    country,
    comments,
    the_geom,
    sensor_id,
    freq
) VALUES
    (2716, 2313, 'ZZ GYPSY 2', -124.0147, 49.2283, 156, NULL, NULL, NULL, 'BC', NULL, NULL, NULL, NULL, '1-hourly'),
    (2717, 2314, 'QUERY ONE STATION PEER', -124.0147, 49.2283, 156, NULL, NULL, NULL, 'BC', NULL, NULL, NULL, NULL, '1-hourly'),
    (2718, 2315, 'QUERY ONE STATION EMPTY', -124.0147, 49.2283, 156, NULL, NULL, NULL, 'BC', NULL, NULL, NULL, NULL, '1-hourly');

INSERT INTO meta_vars (
    vars_id,
    network_id,
    unit,
    "precision",
    standard_name,
    cell_method,
    long_description,
    net_var_name,
    display_name,
    short_name
) VALUES
    (496, 12, 'mm', NULL, 'lwe_thickness_of_precipitation_amount', 'time: sum', 'depth of water-equivalent rain', 'precipitation', 'Precipitation Amount', 'lwe_thickness_of_precipitation_amount_sum'),
    (497, 12, 'celsius', NULL, 'air_temperature', 'time: point', NULL, 'temperature', 'Temperature (Point)', 'air_temperature_point'),
    (498, 12, 'celsius', NULL, 'air_temperature', 'time: mean within months time: mean over years', 'Climatological mean temperature', 'temperature_climatology', 'Temperature Climatology', 'air_temperature_climatology'),
    (499, 12, 'm s-1', NULL, 'wind_speed', 'time: mean', NULL, 'wind_speed', 'Wind Speed (Mean)', 'wind_speed_mean');

INSERT INTO obs_raw (
    obs_raw_id,
    obs_time,
    mod_time,
    datum,
    vars_id,
    history_id
) VALUES
    -- Production-derived target-station temperature observations.
    (194852502, '2006-02-10 15:00:00', '2011-08-29 12:13:18.197183', 9.69999981, 497, 2716),
    (194852505, '2006-02-10 18:00:00', '2011-08-29 12:13:18.197183', 4.80000019, 497, 2716),
    (194852517, '2006-02-11 06:00:00', '2011-08-29 12:13:18.197183', 4.5999999, 497, 2716),
    (194852547, '2006-02-12 12:00:00', '2011-08-29 12:13:18.197183', 9.19999981, 497, 2716),

    -- Network variables observed at the peer station only.
    (194852600, '2006-02-10 15:00:00', '2011-08-29 12:13:18.197183', 0.0, 496, 2717),
    (194852601, '2006-02-10 15:00:00', '2011-08-29 12:13:18.197183', 2.5, 499, 2717),
    (194852602, '2006-02-10 18:00:00', '2011-08-29 12:13:18.197183', 1.2, 496, 2717),
    (194852603, '2006-02-10 18:00:00', '2011-08-29 12:13:18.197183', 3.1, 499, 2717),

    -- Target-station climatological value for the climo=True branch.
    (194852604, '2006-02-10 15:00:00', '2011-08-29 12:13:18.197183', 7.0, 498, 2716);
