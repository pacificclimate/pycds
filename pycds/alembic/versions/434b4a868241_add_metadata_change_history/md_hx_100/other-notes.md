# Other notes and questions

## History table PK

We could, possibly with some benefits, change the PK of the history table from the 
current shared monotone serial `history_id` to a pair `(metadata_id, history_index)`, where
`metadata_id` is the current metadata id (identifying a single item), and 
`history_index` is a value that is serial (1, 2, 3, ...) for any given `metadata_id` 
and gives a position in the history sequence for that one metadata item.

This might have some value in enabling simple responses such as "How much history 
does this item have?"

## Table column ordering

At present we reorder the columns of the primary table, inserting some history cols 
(namely, `create_time`, `creator`) before them. This would make queries that
depend on column order behave incorrectly.

I think we can preserve the original column order and merely suffix new cols at the end. 

## Avoiding duplication between primary and history tables

We do not have to duplicate the latest record for each item; we can store the latest 
state only in the primary and any past states in the history table. This makes full 
history queries more trouble, since we will need to take the union of queries against 
both tables. Furthermore, the present model has the primary and secondary table with 
different columns. Possible responses:

- Query against primary table supplements columns returned from it with additional 
  computed columns to make it compatible with the history table.
- Query against history table drops columns returned from it to make it compatible 
  with the primary columns. This is **not workable** at minimum because the unique history 
  FK would be missing.
- Alter the primary table to have the same columns as the history table.

### Alternative: Compute missing columns

```postgresql
SELECT *, FALSE AS deleted, ??? AS b_hx_id, ??? AS a_hx_id FROM b 
UNION
SELECT * FROM b_hx
```
Problems: 
- `??? AS b_hx_id`: Need to compute the next hx id in `b` (or, if we adopt 
  the above, the next hx index for that item -- probably harder).
- `??? AS a_hx_id`: Need to compute current FK (or FK pair) for given `a_id` (ref). 
  Not hard, already solved.

### Alternative: Extend primary table columns

Exposes things that might be better "hidden" in the history table.

Makes the trigger functions somewhat more complicated and less well separated 
functionally.
