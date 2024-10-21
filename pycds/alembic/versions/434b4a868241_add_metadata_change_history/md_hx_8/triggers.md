# Triggers

Notes:
- Table (view?) name does not need to be provided, since it is available via 
  `TG_TABLE_NAME`. This could reduce the amount of data that needs to provided via 
  `TG_ARGV`.

## View operations to hx table

```postgresql
CREATE TRIGGER t100_view_ops
    INSTEAD OF INSERT OR DELETE OR UPDATE
    ON a
    FOR EACH ROW
EXECUTE FUNCTION view_ops_to_hx_table('a', 'a_id');

CREATE TRIGGER t100_view_ops
    INSTEAD OF INSERT OR DELETE OR UPDATE
    ON b
    FOR EACH ROW
EXECUTE FUNCTION view_ops_to_hx_table('b', 'b_id');
```

## Hx table: Fill in history id's; referential integrity for referenced items

````postgresql
CREATE TRIGGER t100_add_check_foreign_keys
   BEFORE INSERT 
   ON b_hx
   FOR EACH ROW
EXECUTE FUNCTION add_check_foreign_keys($${{a, a_id}}$$);
````

## Hx table: Referential integrity on item delete


```postgresql
CREATE TRIGGER t200_mark_delete_enforce_ref_integ_before
    BEFORE INSERT OR UPDATE OR DELETE
    ON a_hx
    FOR EACH ROW
EXECUTE FUNCTION mark_delete_enforce_ref_integ_before('a', 'a_id');

CREATE TRIGGER t200_mark_delete_enforce_ref_integ_before
    BEFORE INSERT OR UPDATE OR DELETE
    ON b_hx
    FOR EACH ROW
EXECUTE FUNCTION mark_delete_enforce_ref_integ_before('b', 'b_id');
```

```postgresql
CREATE TRIGGER t201_mark_delete_enforce_ref_integ_after
   AFTER DELETE
   ON a_hx
   FOR EACH ROW
EXECUTE FUNCTION mark_delete_enforce_ref_integ_after('a', 'a_id');

CREATE TRIGGER t201_mark_delete_enforce_ref_integ_after
   AFTER DELETE
   ON b_hx
   FOR EACH ROW
EXECUTE FUNCTION mark_delete_enforce_ref_integ_after('b', 'b_id');
```


