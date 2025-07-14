#!/bin/bash
# Argument: Db connection string
connection_string=$1; shift

psql "${connection_string}" <<EOF
with by_pk as (
  select obs_raw_id ids from obs_raw_hx order by obs_raw_id limit 1000
),
by_hx_id as (
  select obs_raw_id ids from obs_raw_hx order by obs_raw_hx_id limit 1000
)
select (select array_agg(ids) from by_pk) = (select array_agg(ids) from by_hx_id)
;
EOF