# Implement history tracking on `obs_raw` and referenced tables

TO DO: Apply history tracking method from /md_hx_101 to 'obs_raw' and the tables it 
directly references.

Outline:

1. Add history tracking trigger functions to schema crmp
   1. `mdhx_primary_ops_to_hx`
   2. `mdhx_add_foreign_hx_keys`
2. Create history tables for `meta_history` and `meta_vars`.
3. Create triggers on them, but only
   1. ON primary: `mdhx_primary_ops_to_hx()`
   2. (skip hx table triggers)
4. Create history table.
   1. Copy `obs_raw` to `obs_raw_hx`
   2. Add history columns to both, see below
5. Create triggers on obs_raw, obs_raw_hx
   1. ON `obs_raw`: `mdhx_primary_ops_to_hx()`
   2. ON `obs_raw_hx`: `mdhx_add_foreign_hx_keys('{{meta_history, history_id}, 
      {meta_vars, vars_id}}')` 

## Metadata tables

### Primary table

Add (at end of primary table) the following columns, in this order
- `mod_time`
- `creator`

```postgresql
ALTER TABLE meta_history
    ADD COLUMN mod_time timestamp WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    ADD COLUMN creator character varying(64) COLLATE pg_catalog."default" NOT NULL DEFAULT CURRENT_USER
;
```

### History table

Copy primary table to history table, name with suffix `_hx`.

Rename column `mod_time` to `create_time`.

Add (at end of history table) the following columns, in this order
- `<collection>_hx_id`
- `deleted`
- foreign history keys (optional; omit)

### Triggers

On primary table only:

```postgresql
CREATE TRIGGER t100_primary_ops_to_hx
     AFTER INSERT OR DELETE OR UPDATE
     ON <table>
     FOR EACH ROW
EXECUTE FUNCTION mdhx_primary_ops_to_hx();
```

## Table `obs_raw`

### Primary table

Add (at end of primary table) the following columns, in this order
- `creator`
- Note: `mod_time` already exists

### History table

Copy primary table to history table, name with suffix `_hx`.

Rename column `mod_time` to `create_time`.

Add (at end of history table) the following columns, in this order
- `<collection>_hx_id`
- `deleted`
- foreign history keys (optional; omit)

### Triggers

