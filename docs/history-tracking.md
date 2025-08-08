# History tracking, a.k.a. change history

**_Note_**: The term "history" is somewhat overloaded here: it refers both to 
_CRMP station history_ (existing table `meta_history`) and to _history tracking_, the
subject of this document. Unless explicitly stated otherwise in this document,
"history" refers to history tracking, not to table `meta_history`.

## Definition and motivations

History tracking is, in broad terms, any way of recording changes to records 
in a database, in such a way that that history can be examined at a later point. History 
tracking can serve a variety of purposes. Those purposes depend on the needs of the user 
base and the database managers, and may include security auditing and "time travel". 
Other considerations, such as searchability and memory use may also come into play.
Depending on the purpose and other considerations, different implementations are suitable.

In our case, history tracking is part of a slightly larger endeavour labelled "version 
control", in which updates to records in the database are expected from time to time, 
and we need to track such updates. 

The canonical use case is as follows: A researcher downloads data from our database,
performs an analysis using it, and publishes their conclusions. Time passes,
during which some of the data downloaded by the researcher is updated. Later, a
reanalysis of the same data is required. In order to do this, it is necessary to be
able to reconstruct the state of the database at the time of the download. The current
state of the database is no longer the same.

Expected updates to the CRMP database come in least three distinct flavours:

1. New records inserted by `crmprtd` and via other routes (e.g., manual imports).
2. Updates to existing record sets due to QA processes performed by CRMP program 
  participants (more or less, "networks"). A typical scenario is: "Raw" observations 
  are published in near real-time. QA'd observations, which are updates to the raw 
  observations, and may include deletions, arrive after a significant (multi-month) time 
  lag. The QA'd dataset nominally replaces the raw dataset, but we must retain the raw 
  dataset for future reference. 
3. PCIC scientists review and update observations or metadata or both. As with the QA 
   updates above, these updates must be distinguishable from the unmodified records.

Form (1) is of course already happening. Forms (2) and (3) have been agreed to as part 
of our workplan for the last several years.

Requirements:
- To be able easily to reconstruct the state of the database at any past date, i.e.,
  to "time travel".
- To be able to determine what user made any given change, and at what time.
- To be able easily to search the change history of a record or an entire table, subject
  to various conditions.
- Not to require any significant changes (ideally, no changes whatsover) to existing
  queries or usages of the database.

Non-requirements:
- It is not required to minimize memory footprint.

## Current status

As of revisions a59d64cf16ca and 8c05da87cb79, changes (insert, update, delete) to the 
following tables are recorded in a separate history table. History tracking enables 
previous states of the database to be fully reconstructed.

- `meta_network`
- `meta_station`
- `meta_history`
- `meta_vars`
- `obs_raw`

History tracking is straightforward to apply to other tables, so this list may 
expand with future changes.

## Implementation

Because of the purposes PCIC has for history tracking, we have chosen an 
implementation with the following characteristics:

- History is kept within the database (not in external files or databases). This 
  enables fast, simple queries to the history.
- History enables easy reconstruction of past states.

### Terminology

#### Basic terms

These terms are used constantly in discussions about history tracking.

**Original table**: A table in CRMP prior to history tracking being applied to it.

**Main table**: An original table after history tracking has been applied to it. Its name 
and all existing uses of it are unchanged.

**History table**: A new table that tracks changes to the main table. A history table 
is append-only.

#### Additional terms

These terms are used for more elaborate discussions about history tracking.

**Collection**: A collection of _(collection) items_. This term refers to both the
table that exposes the latest version of each item and the history
table that contains the history of all items in the collection.

**(Collection) Item** : A datum that contains (meta)data, identified by a stable 
_item id_. An item is mutable, that is, its existence and content can be changed
(created, updated, deleted) as well as read. An item may have more than one history
record associated with it, and its current state is presented in the main table.

**(Collection) Item Id**: A value that identifies a unique _item_ within a _collection_.
Typically an integer, drawn from a sequence. The item id is typically the primary key of the main table.

### Main tables and history (tracking) tables

Main tables are tables that already exist(ed) in CRMP, prior to the 
implementation of history tracking. Those we are particularly concerned with are 
listed above and in the table below. 

History tables are adjunct tables that record the history of main tables. These tables 
are named consistently by suffixing the corresponding main table name with `_hx`. Thus:

| Main table     | History table     |
|----------------|-------------------|
| `meta_network` | `meta_network_hx` |
| `meta_station` | `meta_station_hx` |
| `meta_history` | `meta_history_hx` |
| `meta_vars`    | `meta_vars_hx`    |
| `obs_raw`      | `obs_raw_hx`      |


#### Main tables

Each main table holds the current version of the records. A main table is, after 
history tracking is applied, a slightly modified version of the original table, with 
two new columns that expose some useful history information, namely modification 
time and modification user id. Otherwise they are unchanged, and function exactly as 
before, including foreign keys, constraints, indexes and other features.

In outline, a main table is derived from the original table as follows:

| Original table         | Main table (same name)  | Comments                                                       |
|------------------------|-------------------------|----------------------------------------------------------------|
| _original columns ..._ | _original columns ..._  | Includes FKs.                                                  |
|                        | `mod_time: timestamp`   | Time this record was created or modified to its current value. |
|                        | `mod_user: varchar(64)` | User who created or modified this record to its current value. |

#### History tables

Each history table holds all versions of all records in the main table, including the 
current version, also held in the main table. (This is duplication and denormalization, 
but it makes many operations simpler.) The columns of the history table include those 
of the main table, plus additional history-specific columns.

A history table is append-only. That is, no updates or deletions are made to it in 
the normal course of history tracking. (Updates and deletes are extraordinary and 
should only be resorted to in order to test things or to correct errors.) Because it 
is append-only, history records are stored in the order of the changes made.

In outline, a history table is structured as follows:

| Main table (`xxx`)       | History table (`xxx_hx`)  | Comments                                                                                                                  |
|--------------------------|---------------------------|---------------------------------------------------------------------------------------------------------------------------|
| _main table columns ..._ | _main table columns ..._  | Includes main table PK (which is not the PK of this table), `mod_time`, `mod_user`.                                       |
|                          | `deleted: boolean`        | True when record is deleted from main table.                                                                              |
|                          | `xxx_hx_id: integer` (PK) | Primary key of _history table_.                                                                                           |
|                          | `yyy_hx_id: integer` (FK) | For each FK in the main table to another table `yyy`, there is a parallel FK to the history table `yyy_hx` for that table |
|                          | _other history FKs ..._   |                                                                                                                           |

Notes regarding primary keys (PKs) and foreign keys (FKs):

- The PK of the main table is a column in the history table, but is _not_ the PK of the 
  _history table_.
- The PK of the history table is a new column, and functions in the usual way.
- FKs in the main table are retained in the history table as columns, but they are not 
  FKs. (They  cannot be FKs because deletions from the foreign main table would raise 
  errors with these necessarily persistent references to it.)
- Any FKs in the main table are paralleled with matching FKs to the corresponding history 
  tables. This makes it possible to reconstruct exactly the same state of the database 
  at any point for any given history record in any table. The history table FKs allow 
  us to find any related records at the same historical point in the database. Thus it 
  is a complete history, not isolated to a single table.

#### Naming conventions

Consistent naming simplifies both human comprehension and programming using history 
tables. In particular, the trigger functions (see below) that implement dynamic history 
tracking rely on this consistency.

Original (pre-existing) tables in the CRMP database do not follow a fully consistent 
naming pattern for primary and foreign keys. For example, all metadata tables, `meta_xxx`, 
typically have primary keys named `xxx_id` (no `meta` prefix). 
Slightly inconsistently with that, table `obs_raw` has primary key `obs_raw_id`. 
Therefore, for the purposes of forming and maintaining a history table from a main table,
we need two pieces of information: the main table name and the main table's primary 
key name. 

For history tables, we form all (new) names completely consistently. Let `xxx` 
be the name of the main table. Then we form the following history names:

- History table name: `xxx_hx`
- History table PK name: `xxx_hx_id` (note: the full main table name is used in this 
  name, not a derivative of the main table PK name)
- Foreign key name in a history table: Must be identical to the name of the PK in the 
  referenced table.
- Names of history columns: must always be `mod_time`, `mod_user`, `deleted`, and the 
  PK and FK names noted above.

The trigger functions (see below) also need the name of the main table FK, and this is 
supplied as an argument to them.

### Dynamic history tracking: trigger functions

Actual history tracking while the database is in use, i.e., when records are inserted, 
modified or deleted in tracked tables, is implemented by trigger functions attached 
to the main tables and history tables.

Three separate but coordinated trigger functions are used for each collection. They are:

| Trigger name                   | Attached to   | Purpose                                                                                                                       |
|--------------------------------|---------------|-------------------------------------------------------------------------------------------------------------------------------|
| `t100_primary_control_hx_cols` | Main table    | Sets the values of `mod_time` and `mod_user`, overriding any values set by the user.                                          |
| `t100_primary_ops_to_hx`       | Main table    | Appends a history record to the corresponding history table when a record is inserted, updated, or deleted in the main table. |
| `t100_add_foreign_hx_keys`     | History table | Sets the foreign keys in the history table when a new history record is appended.                                             |

These trigger functions could be consolidated into a single function, but to partition 
them in this way was conceptually simpler during experimentation, implementation and 
testing, and may ease future changes. OTOH, it may be worth consolidating them now that 
implementation has settled.

Note: The term "primary" is a synonym for "main" (as in "main table") that was 
superseded in later usage.
