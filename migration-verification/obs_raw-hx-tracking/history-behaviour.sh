#!/bin/bash
# Argument: Db connection string
connection_string=$1; shift

# Arguments: values to be inserted in database
obs_time=${1:-"2100-01-01 00:00"}; shift
vars_id=${1:-"1200"}; shift
history_id=${1:-"2828"}; shift
datum=${1:-"99"}; shift

ins_row="'${obs_time}', ${vars_id}, ${history_id}, ${datum}"

psql "${connection_string}" -a <<EOF
-- Check INSERT
INSERT INTO obs_raw(obs_time, vars_id, history_id, datum) VALUES (${ins_row});

SELECT 'INSERT: ' ||
  CASE
  WHEN (obs_time, vars_id, history_id, datum, deleted) = (${ins_row}, FALSE) THEN 'VERIFIED'
  ELSE 'FAILED' END AS outcome
FROM obs_raw_hx ORDER BY obs_raw_hx_id DESC LIMIT 1;

-- Check UPDATE
UPDATE obs_raw SET datum = datum + 1 WHERE obs_time = '${obs_time}';

SELECT 'UPDATE: ' ||
  CASE WHEN (obs_time, vars_id, history_id, datum-1, deleted) = (${ins_row}, FALSE) THEN 'VERIFIED'
  ELSE 'FAILED' END AS outcome
FROM obs_raw_hx ORDER BY obs_raw_hx_id DESC LIMIT 1;

-- Check DELETE
DELETE FROM obs_raw WHERE obs_time = '${obs_time}';

SELECT 'DELETE: ' ||
  CASE
  WHEN (obs_time, vars_id, history_id, datum-1, deleted) = (${ins_row}, TRUE) THEN 'UPDATE: VERIFIED'
  ELSE 'UPDATE: FAILED' END AS outcome
FROM obs_raw_hx ORDER BY obs_raw_hx_id DESC LIMIT 1;

-- Clean up
SELECT count(*) = 3 FROM obs_raw_hx WHERE obs_time = '${obs_time}';
DELETE FROM obs_raw_hx WHERE obs_time = '${obs_time}';
SELECT count(*) = 0 FROM obs_raw_hx WHERE obs_time = '${obs_time}';
EOF