#!/bin/bash
# Argument: Db connection string
connection_string=$1; shift

psql "${connection_string}" <<EOF
with
obs_raw_sample as (select * from obs_raw order by obs_raw_id limit 1000),
obs_raw_hx_sample as (select * from obs_raw_hx order by obs_raw_id limit 1000)
select every(

      o.obs_time is not distinct from hx.obs_time
  and o.datum is not distinct from hx.datum
  and o.vars_id is not distinct from hx.vars_id
  and o.history_id is not distinct from hx.history_id

)
from obs_raw_sample o join obs_raw_hx_sample hx on o.obs_raw_id = hx.obs_raw_id
;
EOF