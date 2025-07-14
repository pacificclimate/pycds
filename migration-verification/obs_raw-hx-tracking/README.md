# Migration verification for `obs_raw` history tracking

These scripts support verification activities when applying history tracking to table
`obs_raw`. For an example, see https://redmine51.pcic.uvic.ca:4433/issues/2298#note-55.
These scripts wrap the manual queries and comparisons documented there.

## Verify common metadata queries

These queries verify that the main metadata tables remain identical after the
migration is executed.

**Before** migration:
1. Run `nsv-pre.sh`

**After** migration:
1. Run `nsv-post.sh`
2. Verify that no significant changes are recorded in files `networks-diff.out`,
   `stations-diff.out`, `variables-diff.out`.

## Verify `obs_raw_hx` ordering

Table `obs_raw_hx` must be in the same order as `obs_raw`. This script checks that. 
The result of the query should be `true`.

**After** migration:
1. Run `history-ordering.sh`

## Verify equality of `obs_raw` and `obs_raw_hx` contents

Table `obs_raw_hx` must have the same contents as `obs_raw`. This script checks that.
The result of the query should be `true`.

**After** migration:
1. Run `history-equality.sh`

## Performance testing

Script: `insert-performance-test.sh`

This script tests performance on inserts to table `obs_raw`. Performance is expected
to decline by at least a factor of 2 because every insert to that table is accompanied
by an insert to `obs_raw_hx`, plus the overhead of history tracking unique id maintenance.

Usage: `insert-performance-test.sh connection_string [num_thousand_inserts [obs_time [vars_id [history_id [datum]]]]]`
where
- `connection_string` is the connection string for the database to be tested. Required.
- `num_thousand_inserts` is the number of thousand records to be inserted into 
  `obs_raw`. Two attempts are made for each record, resulting in one successful insert 
  and one failed. Default 4.
- `obs_time` is the `obs_time` to be used. Default `2100-01-01` + 1 ms increment for 
  each record.
- `vars_id` is the `vars_id` to be used. Default `1200`.
- `history_id` is the `history_id` to be used. Default `2828`.
- `datum` is the `datum` (observation value) to be used. Default `99`.

Results are written to file `insert-performance-test-${date_str}.out` where `date_str` 
is the date-time the script is executed.

**Before** migration:
1. Run `insert-performance-test.sh`.

**After** migration:
1. Run `insert-performance-test.sh`.
2. Compare times for inserts recorded in the .out files.

