BEGIN;

CREATE TABLE subset.meta_history AS 
(
	SELECT * 
	FROM crmp.meta_history
	WHERE history_id % 100 = 16
	AND province = 'BC'
	LIMIT 50
);

-- SELECT * FROM subset.meta_history;

CREATE TABLE subset.meta_station AS 
(
	SELECT *
	FROM crmp.meta_station
	WHERE station_id IN 
	(
		SELECT station_id 
		FROM subset.meta_history
	)
);

-- SELECT * FROM subset.meta_station;

CREATE TABLE subset.meta_network AS 
(
	SELECT *
	FROM crmp.meta_network
	WHERE network_id IN 
	(
		SELECT DISTINCT network_id 
		FROM subset.meta_station
	)
);

-- SELECT * FROM subset.meta_network;

CREATE TABLE subset.meta_network_geoserver AS 
(
	SELECT *
	FROM crmp.meta_network_geoserver
	WHERE network_id IN 
	(
		SELECT network_id 
		FROM subset.meta_network
	)
);

-- SELECT * FROM subset.meta_network_geoserver;

CREATE TABLE subset.crmp_network_geoserver AS 
(
	SELECT *
	FROM crmp.crmp_network_geoserver
	WHERE history_id IN 
	(
		SELECT history_id 
		FROM subset.meta_history
	)
);

-- SELECT * FROM subset.meta_network_geoserver;
    
CREATE TABLE subset.obs_raw AS
(
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 0 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 1 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 2 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 3 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 4 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 5 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 6 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 7 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 8 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 9 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 10 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 11 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 12 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 13 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 14 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 15 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 16 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 17 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 18 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 19 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 20 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 21 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 22 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 23 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 24 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 25 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 26 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 27 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 28 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 29 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 30 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 31 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 32 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 33 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 34 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 35 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 36 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 37 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 38 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 39 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 40 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 41 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 42 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 43 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 44 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 45 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 46 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 47 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 48 LIMIT 1 ) LIMIT 50) UNION
    (SELECT * FROM crmp.obs_raw WHERE history_id = (SELECT history_id FROM subset.meta_history ORDER BY history_id OFFSET 49 LIMIT 1 ) LIMIT 50)
);

-- CREATE TABLE subset.obs_raw AS 
-- (
-- 	SELECT *
-- 	FROM crmp.obs_raw
-- 	WHERE history_id IN 
-- 	(
-- 		SELECT history_id 
-- 		FROM subset.meta_history
-- 	)
-- 	LIMIT 10000
-- );

-- SELECT * FROM subset.obs_raw;

CREATE TABLE subset.meta_vars AS 
(
	SELECT *
	FROM crmp.meta_vars
	WHERE network_id IN 
	(
		SELECT network_id 
		FROM subset.meta_network
	)
);

-- SELECT * FROM subset.meta_vars;

CREATE TABLE subset.obs_raw_native_flags AS 
(
	SELECT *
	FROM crmp.obs_raw_native_flags
	WHERE obs_raw_id IN 
	(
		SELECT obs_raw_id 
		FROM subset.obs_raw
	)
);

-- SELECT * FROM subset.obs_raw_native_flags;

CREATE TABLE subset.meta_native_flag AS 
(
	SELECT *
	FROM crmp.meta_native_flag
	WHERE native_flag_id IN 
	(
		SELECT native_flag_id 
		FROM subset.obs_raw_native_flags
	)
);

-- SELECT * FROM subset.meta_native_flag;

CREATE TABLE subset.obs_raw_pcic_flags AS 
(
	SELECT *
	FROM crmp.obs_raw_pcic_flags
	WHERE obs_raw_id IN 
	(
		SELECT obs_raw_id 
		FROM subset.obs_raw
	)
);

-- SELECT * FROM subset.obs_raw_pcic_flags;


CREATE TABLE subset.meta_pcic_flag AS 
(
	SELECT *
	FROM crmp.meta_pcic_flag
	WHERE pcic_flag_id IN 
	(
		SELECT pcic_flag_id 
		FROM subset.obs_raw_pcic_flags
	)
);

-- SELECT * FROM subset.meta_pcic_flag;

CREATE TABLE subset.vars_per_history_mv AS 
(
	SELECT DISTINCT history_id, vars_id
	FROM subset.obs_raw
);

-- SELECT * FROM subset.vars_per_history_mv;

CREATE TABLE subset.climo_obs_count_mv AS 
(
    SELECT count(*) AS count, obs_raw.history_id
    FROM subset.obs_raw
    NATURAL JOIN subset.meta_vars
    WHERE meta_vars.cell_method::text ~ '(within|over)'::text
    GROUP BY obs_raw.history_id
);

-- CREATE TABLE subset.climo_obs_count_mv AS 
-- (
-- 	SELECT *
-- 	FROM crmp.climo_obs_count_mv
-- 	WHERE history_id IN 
-- 	(
-- 		SELECT history_id 
-- 		FROM subset.meta_history
-- 	)
-- );

-- SELECT * FROM subset.climo_obs_count_mv;

SELECT 'CREATING obs_count_per_month_history_mv';
    
CREATE TABLE subset.obs_count_per_month_history_mv AS 
(
SELECT count(*) AS count,
       date_trunc('month'::text, obs_raw.obs_time) AS date_trunc,
       obs_raw.history_id
FROM subset.obs_raw
GROUP BY date_trunc('month'::text, obs_raw.obs_time), obs_raw.history_id
);


-- CREATE TABLE subset.obs_count_per_month_history_mv AS 
-- (
-- 	SELECT *
-- 	FROM crmp.obs_count_per_month_history_mv
-- 	WHERE history_id IN 
-- 	(
-- 		SELECT history_id 
-- 		FROM subset.meta_history
-- 	)
-- );

-- SELECT * FROM subset.obs_count_per_month_history_mv;

CREATE TABLE subset.obs_with_flags AS 
(
	SELECT *
	FROM crmp.obs_with_flags
	WHERE obs_raw_id IN
	(
		SELECT obs_raw_id
		FROM subset.obs_raw
	)
);

-- SELECT * FROM subset.obs_with_flags;

CREATE TABLE subset.meta_sensor AS 
(
	SELECT *
	FROM crmp.meta_sensor
);

-- SELECT * FROM subset.meta_sensor;

-- ROLLBACK;
COMMIT;