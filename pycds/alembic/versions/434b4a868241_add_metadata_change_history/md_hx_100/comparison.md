# Comparison of approaches

We have two approaches to implementing v.c. for metadata tables. They differ in 
whether the primary interface object is a view or a table.

The two approaches have much in common:

- The primary, "user-facing" interface is a table-like object (a view or an actual table)
  that exposes the most recent state of each metadata object in a collection.
- The history of each metadata object is stored in an adjunct history table which
  contains one record for each past state and the current state of each metadata
  object in a collection. The history table is append-only.
- Trigger functions are used to mediate between primary and history table, and to fill
  in required history data that must be added post-hoc.

Their differences are summarized in the table below.

|                              | View                             | Table               |
|------------------------------|----------------------------------|---------------------|
| Connection to non-md tables  | Requires modifying non-md tables | No changes required |
| Primary keys                 | No (1)                           | Yes                 |
| References between primaries | Shared cols; not FKs (1)         | True FKs            |
| References between hx tables | True FKs                         | True FKs            |
| Data normalized?             | Yes                              | No                  |
| Query speed                  | Slightly increased               | Unchanged           |
| TF: primary -> history       | ✔ ; more complicated             | ✔ ; simpler         |
| TF: add history FKs          | ✔ ; more complicated             | ✔ ; simpler         |
| TF: Maintain ref. integrity  | ✔ ; complicated                  | ✗                   |

(1) Views do not support primary keys or foreign keys.
(2) TF = trigger function.

The primary-table implementation offers several benefits at the cost of a small amount of 
data duplication between the primary and history table (namely, the latest state of 
each metadata item is stored in both tables). 
The main benefit is that no changes
need be made to any non-metadata tables to accommodate the history feature.
Additionally, it permits a simpler, smaller implementation.