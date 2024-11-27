# Extension to observations

## Intro

One possible approach to VC for observations is to extend the existing approach in 
some way. The existing approach includes two variants: 

- primary-table: the primary (non-hx) interface to metadata is a table
- primary-view: the primary (non-hx) interface to metadata is a view

We can consider taking either approach (or others, not considered here) to placing 
the observation table (obs_raw) and possibly other non-metadata tables under VC. 

## Overview

We can consider two aspects of this extension to observations:
- VC on the observation table
- Connection to metadata tables

In both approaches under consideration, the observations are backed by a history 
table which contains (directly or effectively) a record of past and current states of 
each observation item.

Let's take a quick overview of the implications of each approach.

Primary-table approach
- Alternative 1: Apply exactly the same design, with duplication of latest value for 
  each item in primary and history tables. Consequences: 
  - Immediate doubling of storage required for the observations, since the latest 
    observation is duplicated in each table. This may 
    or may not be an issue, since the database is large but not extremely large.
- Alternative 2: Store only past states of each observation in the 
  history table, and the current state only in the primary table. Consequences:
  - Queries about history become more complicated, requiring a 
    concatenation of the primary and history tables to form a full history. 
  - This approach does not "freeze" the state of the metadata at the time of latest 
    update unless some additional FKs (to each relevant metadata history table) are 
    added to the observation table.
- Queries to observation table are unchanged in speed or form.

Primary-view approach:
- No immediate doubling of storage, since observation states are stored exactly once 
  in the history table.
- The intervening view query may in itself slow queries. (Depends on how good the 
  query planner is with the particular view and external queries together.)
- Not possible to have primary keys in a view. Therefore:
  - Indexes must reside in history table. This will slow down queries when there are 
    many history records for a single item.
- Not possible to have foreign keys to/from a view. Therefore:
  - Reference relationships ("psuedo-FKs") must be mediated by trigger functions that
    perform "get latest" queries on the history table.
  - Any other table that wishes to have a FK to the observation table must do 
    something similar.

## Recommendation

The limitations imposed on PKs and FKs by views seem to rule out the primary-view 
approach as a practical solution.

That leaves us with the primary-table approach. If the sudden increase in storage size 
for the observations table is tolerable, then the first alternative -- to duplicate 
the latest observation item in both primary and history tables -- is simpler and more 
desirable. 
If not, it will be necessary to modify the observations table further to support the 
desirable (and possibly necessary) history FK relationships.

Implementing either choice will be straightforward.



