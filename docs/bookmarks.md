# Bookmarks aka version tags aka commits aka ...

Let's use the term "bookmark" for now. Single word, not otherwise used, seemingly intuitive meaning, easy to search/replace.

We want to be able to "bookmark" a state of the database (i.e., bookmark a point in its history) with an identifier that can be used to reconstruct that state after subsequent changes to the database. The nominal use case is "bookmark the present state," but there are definitely other possibilities.

## Terminology

**Bookmark**: A named object that points at a set of history records, one per history-tracked table. This definition conflates the notion of a bookmark proper and the association of it to history records, which actually are distinct, but it will suffice for now.

## Facts and assumptions

**Facts**

- History tables are append-only. 
- Each history table records the changes made to the entire collection *in temporal order of the changes*. 
- Each successive update to the collection is recorded by adding a record at the end, which so that temporal order is also the order by 0ascending unique history id. 

**Assumptions**

- No existing record in the history table is ever modified.

**Therefore**

- If a bookmark is associated to a record in a history table, it represents the history of that collection up to that point in time. A bookmark can be thought of by analogy with a Git tag, in the sense that both are pointers to a specific state of the relevant items.
- Two such bookmark associations, say $B_1$ and $B_2$, bracket a set of changes recorded in the history table. The delta between them is exactly those changes recorded in the history table, in history id order, between $B_1$ (exclusive) and $B_2$ (inclusive).

**FIXME**: Bookmark associations can be, and most naturally are, stored in order of the association operations, that is, temporally. Therefore we can read out a series of successive changesets simply by examining the bookmark associations in the order they are made. 
- However, that's not true if we allow bookmarking of non-latest states, which is probably going to be desirable. We can't think ahead perfectly. Hmmm.
- Alternative ordering for bookmarks: In the order they occur according to the history table id, that is in history table temporal order. These had better be consistent across all history tables.

## Basic bookmark operations

We believe firmly at present that the operation of "bookmark this state" should be applied atomically across all history-tracked tables. That is, a bookmark does not associate to a single history table record, but rather to a contemporaneous set of records, one per history table, that represent a real point in time in the history of the database, and that this is an atomic operation.

### Bookmark the database state

**Motivation**: This is the fundamental operation in bookmarking.

**Operation**: Associate a bookmark to a set of history records, one per history-tracked table, that represent a real point in time of the history of the database. This operation is atomic.

Notes:

- When a bookmark is created at the actual point in time when the bookmarked database state exists, the bookmark is guaranteed to be valid.
- It is possible that we will want to bookmark a *past* state of the database after the fact. In that case, we will need a validity check to ensure that the set history records associated to the bookmark actually represent a true past state, and not just an arbitrary and inconsistent selection of history records.

### Bracket (or group) a set of updates

**Motivation/scenario**: A set of related updates are received or made all at one time. The canonical case is a QA update of a large set of observations. (In other cases, e.g., when a scientist is updating things, it will take a certain amount of discipline to make sure that the updates are batched together like this.)

**Operation**: *Within a transaction (i.e., atomically)*: 

- Bookmark the database with bookmark $B_1$.
- Perform updates.
- Bookmark the database with bookmark $B_2$.

Changes between $B_1$ (exclusive) and $B_2$ (inclusive) are exactly and only those changes made in the updates. This is due to: 

- their isolation in the update transaction (so no other operations interleaved); 
- the fact that change records are appended to the history tables in temporal order of change operations; therefore the last change operation is recorded at the end of the relevant history table.

We call this operation bracketing or grouping, and denote it $Bracket(B_1, U, B_2)$, where $B_1$ and $B_2$ are the bookmarks and $U$ is the set of updates.

**For further discussion and analysis**:

- Since the bookmarks for bracketing are directly related, we would probably do better to use a single bookmark prefix $B$, and allow the system to construct bookmarks $B_1$ and $B_2$ from $B$. We can then define  $Bracket(B, U) = Bracket(B_1, U, B_2)$, where $B_1$ and $B_2$ are the constructed bookmarks.
- **BAD IDEA?** A bracket bookmark association could additionally include a time-of-bookmarking component, so that the same bookmark prefix can be used to construct bracket bookmarks for multiple applications of it. This time-of-bookmarking component can, in fact, be placed in the association, not the bookmark itself. I don't even know what this series of words means any more. 
	- Maybe better to let the bookmark name include any additional data needed than to try too hard to anticipate usage. 
	- Not only that but there are doubtless usages in which time of bookmarking is not relevant.
	- However, it is conceivable that, given that time of bookmarking is only a single additional column and might generically be useful -- as part of *bookmarking history* -- that we should include it anyway. 
	- **TODO**: think through the concept and possible implementations of bookmarking history.


## Applications

### Database state reconstruction or Rollback

#### Outline

The design of history tracking makes easy in concept to reconstruct the complete state of the database from a bookmark. This is in fact what bookmarking is for. 

Q: How do we do that? 
A: Query the latest state of each item in the collection whose history id is less than or equal to the id of the record the bookmark points at. One such query is the following: For each collection (i.e., each history-tracked table)
```
SELECT DISTINCT ON (collection_item_id) * 
FROM collection_hx 
WHERE collection_hx_id <= collection_hx_id_from_bookmark
ORDER BY collection_hx_id DESC
```
returns the set of collection items that was current as of the bookmark.
#### Implementation considerations

For metadata tables, which have few records, the above query is likely fast. For `obs_raw_hx`, it will scan a huge number of records. To make it perform better, further WHERE conditions may have to be added and possibly judicious indexing on the history tables. But see also below.

Alternatively: Further to the problem of `obs_raw_hx` being enormous, and queries against it therefore taking very long times, here is a possibility, in which we create a separate rolled-back version of the database.

In a separate schema, call it `crmp_rollback`, do the following. Given a bookmark:

- Establish replica of the `crmp` schema (i.e., table definitions without data) including main tables but excluding history tables (history tables are redundant here).
- Include FK relationships, indexes, and other things as needed.
- Duplicate the content of the non-history tracked tables (at the time of of this rollback).
- Populate the history-tracked tables using queries as above.
- Define and populate (one row) a rollback table that contains at least the following information:
	- bookmark id
	- timestamp when this rollback was established
	- id of user creating the rollback
	- any other important information not retrievable with this data
- Possibly make all tables read-only. 
- Possibly make this schema accessible only by a specific role which is granted only to the user(s) who need this version of the database.

And, in the main `crmp` schema, define a stored procedure that does all of the above:

- Given a bookmark id and rollback schema name
- In a valid order (respecting foreign key dependencies)
- With optimized queries insofar as possible 

Once the rollback schema is populated with data reflecting a given point in history, the users with interest in it can query it as if it is the actual CRMP database. It will not in general be wise to allow the users to modify this database, since those modifications will not under any circumstances be propagated to the real database. For experimental purposes, with appropriate, loudly stated caveats, this rule may be relaxed.

Any number of rollback schemas can be established. They do not interact with each other, nor with the live CRMP database. But because each rollback schema will be comparable in size to the live database, we may wish to limit their number and their lifetimes.
### Efficient grouping of data versions

CRMP partners periodically release new versions of a dataset. Such releases typically contain many thousands of observations. A version release amounts to an update to each such datum. 

Releases typically are, in time order: 

1. A raw dataset. This dataset frequently arrives incrementally, via `crmprtd`.
2. A QA-adjusted dataset. This dataset is expected to arrive in one or a few large batches.

Each observation in the second release is a revision to an observation in the first release.
#### An unsatisfactory approach

It is possible to group the observations in such a release individually, by associating a bookmark to each updated item's history record. This requires the same number of bookmark associations as there are observations, i.e., it scales linearly with the number of observations. This is not desirable and should be avoided if at all possible.
#### Bracketing

Bracketing uses only two bookmarks to demarcate the beginning and end of a batch of updates:

- Let $U$ be the set of updates that add the new release.
- Define bookmarks $B_1$ and $B_2$ to demarcate (bracket) the release.
- Perform $Bracket(B_1, U, B_2)$.

Bracketing requires only constant time and space relative to the number of updates within it. It significantly improves both memory footprint and retrieval time.
##### Bracketing for large datasets (QA releases, other updates)

QA releases and other updates are expected to arrive in large batches. If it is not operationally possible to perform the updates for a new release within a single transaction, this approach can generalized to a small number of bracketing operations which together encompass the whole release. This will still significantly reduce the space and time required to bracket and retrieve a large group of observations.
##### Bracketing for regular ingestion (`crmprtd`)

Regular ingestion (via `crmprtd` and related scripts) occurs piecemeal. Typically, dozens to thousands of observations are ingested at a time (hourly, daily, weekly, or monthly, depending on the network). We can use bracketing for each such group of observations ingested. This is still much smaller than a typical QA release, and is linear, not constant space and time in the number of observations, but it is better than bookmarking one observation at a time.

Considerations:

- It will be useful for the bookmarks used to bracket each ingestion to bear a clear and easily queried relationship to each other. This would enable an entire raw dataset to be extracted with simpler and more error-resistant queries. Here is where the idea of a bookmark association including a temporal column (or a more general informational text column) would be useful. 
- In some sense a fully normalized representation for this case includes:
	- A single bookmark associated multiple times to a series of groups of observations.
	- One part (column) of the association distinguishes bracket-start and bracket-end.
	- Another part (column) of the association distinguishes the group. Time of ingestion is a natural discriminator for this, but it may be too restricted for possible more general uses of "many groups labelled by a single bookmark". 
- For more details, see Implementation notes below.
## Implementation notes

We begin to see the outlines of a plausible implementation, as follows.
### Tables 

**Table `bookmarks`**

| Column         | Type        | Remarks                                                                                                                                                                                                                                                                             |
| -------------- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bookmark_id`  | `int`       | PK                                                                                                                                                                                                                                                                                  |
| `name`         | `text`      |                                                                                                                                                                                                                                                                                     |
| ? `comment`    | `text`      | Meaning? Utility?                                                                                                                                                                                                                                                                   |
| ? `network_id` | `int`       | FK `meta_network`. Utility is to distinguish bookmarks for one network from another, and allow a simple, natural name in common, such as 'QA'. Normalizes a common use case, I think. Tempting to make nullable, but caution, nullable columns have frequently been abused in CRMP. |
| `mod_user`     | `text`      |                                                                                                                                                                                                                                                                                     |
| `mod_time`     | `timestamp` |                                                                                                                                                                                                                                                                                     |

Constraints

- unique (`name`, `network_id`)

Questions:

1. Apply history tracking to this table? Reason, utility?

**Table `bookmark_associations`**

Q: Why separate association from bookmark proper? 

A: 
- Brackets share the same bookmark info, but are associated as bracket-begin, bracket-end. 
- We likely want to bracket datasets ingested by `crmprtd` using the same bookmark.
- Nevertheless the normalization here seems a bit awkward, or rather to permit more than valid usage. Some further considering required.

| Column                    | Type                                                                                                          | Remarks                                                                                                                                                                                                                                                                                                                                                             |
| ------------------------- | ------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bookmark_association_id` | `int`                                                                                                         | PK                                                                                                                                                                                                                                                                                                                                                                  |
| `bookmark_id`             | `int`                                                                                                         | FK `bookmarks`                                                                                                                                                                                                                                                                                                                                                      |
| ? `type`                  | ?`enumeration`; ?`int` (FK to type table); ?`text`. Values: `'bookmark'`, `'bracket-begin'`, `'bracket-end'`. | Still some question to the wisdom of encoding this aspect of bookmark usage with a separate column. If we do, enumeration type might be best.                                                                                                                                                                                                                       |
| ? `group`                 | ?`timestamp`; ?`int`; ?`text`                                                                                 | For distinguishing multiple associations of the same bookmark. Specific usages: `crmprtd` ingestion; possibly for regular QA releases. Will require discipline on the part of the user in order not to make a mess, particularly if the discriminator is textual. May need better name; will depend in part on type (`timestamp` vs. more general `int` or `text`). |
| ? `aux_info`              | `text`                                                                                                        | Auxiliary information about the association. Largely motivated by the desire to expand on the meaning of `group`, especially in the case that it is an integer.                                                                                                                                                                                                     |
| `obs_raw_hx_id`           | `int`                                                                                                         | PK `obs_raw_hx`                                                                                                                                                                                                                                                                                                                                                     |
| `meta_network_hx_id`      | `int`                                                                                                         | PK `meta_network_hx`                                                                                                                                                                                                                                                                                                                                                |
| `meta_station_hx_id`      | `int`                                                                                                         | PK `meta_station_hx`                                                                                                                                                                                                                                                                                                                                                |
| `meta_history_hx_id`      | `int`                                                                                                         | PK `meta_history_hx`                                                                                                                                                                                                                                                                                                                                                |
| `meta_vars_hx_id`         | `int`                                                                                                         | PK `meta_vars_hx`                                                                                                                                                                                                                                                                                                                                                   |
| `mod_user`                | `text`                                                                                                        |                                                                                                                                                                                                                                                                                                                                                                     |
| `mod_time`                | `timestamp`                                                                                                   |                                                                                                                                                                                                                                                                                                                                                                     |

Constraints:

- unique (`bookmark_id`, `type`, `group`)

Questions:

1. Apply history tracking to this table? Reason, utility?

### Functions, stored procedures

Since bookmarking is a non-trivial activity, it will be useful to encode it in stored procedures. There is some question of whether some or all of this should instead be utility Python code in the PyCDS repo proper and not SP's within the database, but we'll mix em all up here in one list.

1. Create a bookmark association at current time (current state of database).
2. Check tuple validity.
3. Create a bookmark association from a past state (history tuple). Check validity of tuple.
4. Determine support (see discussion) of an observation. Result is a valid history tuple. Can then create bookmark association to it. 
5. Perform bracketing operation.
	1. Create bookmark(s).
	2. Create bracket-begin association.
	3. Apply updates.
	4. Create bracket-end association.

### Triggers

1. Enforce values of `mod_time`, `mod_user` in `bookmarks` and `bookmark_associations`. (As for history tracking; reuse tf.)
## History tuples, database subset, and validity

Our goal here is to check whether a given set of history id's is valid, i.e., does it represent a real, consistent historical state of the database. This will be useful when attempting to define a bookmark association post hoc.

**History tuples**: We can regard a bookmark association as a tuple of collection history id's, one per collection (hereafter a history id tuple, history tuple, or just tuple).

**Database history subset**: A history tuple defines a subset of the database history, namely all those history records in each history table that occur before the corresponding history id in the tuple. Under the reasonable assumption (see above) that the temporal order of history records is the same as the history id order, "before" here means that history id is less than or equal to the history id in the tuple.

**Validity**: Not all such database subsets, and therefore such tuples, are valid. 

The criterion for validity is essentially referential integrity. That is, within the subset of history records implied by the tuple, all references by a history record in that subset to another history record must also be found in the subset. Otherwise (i.e., when there is a violation of referential integrity within the subset) the database history subset is not valid.

History id tuples are valid iff they imply a valid database history subset.

**Algorithms for checking tuple validity**: Given this definition, algorithms for checking tuple validity are straightforward:

1. The naive algorithm checks every reference in any given subset for presence in the referenced collection history subset. But this is a huge number of records in most cases.
2. A less naive and much faster algorithm relies on the assumption that every actually occurring historical state of the database was valid (quite a reasonable assumption!), and therefore that history tables reflect that. This assumption allows us to check only that reference history id's are less than or equal to the corresponding collection history id in the tuple.

**Applications**: 

1. Creating a (valid) bookmark association post hoc.
2. It is possible that a history tuple may be presented for checkout (see below) that is not known to be valid. 
3. It is also possible that a single point of a single collection may be presented and we wish to construct a valid database historical state from it. Validity criteria allow us to do this.

## Metadata support set

**Note/TODO**: This section may not be all that useful any more ... but I include it for consideration. It may also be overcomplicated ... the support of a set of observations may be more general than is really useful. It might be better to consider the support of only the earliest and latest records in the set, since those effectively bracket the group of observations. But, post-hoc, i.e., non-atomically, that bracketing is too large, so we will need to look at some notion of contiguous groups if that is possible. Oy vey, more work to do here.

The idea of the "metadata support" may prove useful in talking clearly about bookmarking. In particular, it may prove useful in discussing bookmarking or bracketing a set of observations post hoc. From here on, we may abbreviate "metadata support" to "support".

Support enables us to talk in a well-defined, compact way about the metadata relevant to an observation (or set of observations), when the observations are the only handle you have at the outset. More accurately, we should say observation histories, since observations are mutable and not the target of bookmarking.

The support of an observation history record $X$ is the set of metadata (history) records directly relevant to $X$, which is to say directly associated to $X$ by one or more FK links away from the observation. This in fact applies to any history record $X$, but observation histories are the most important and are the most general or complex case.

### Definitions

We define 2 particular cases of support that are especially relevant:

#### Historical support, $Sh(X)$

- The *historical (metadata) support* of observation history record $X$, denoted $Sh(X)$, is the tuple of metadata history records linked to it via history table foreign keys followed directly from one history record to another. 
- There is always exactly one of each metadata history record type (Network, Station, Station History, Variable) in this tuple.
- This tuple is the precise metadata state at the time of creation of $X$. 
- This tuple *does not change* when updates to the corresponding metadata items are made.

We can easily generalize this to a set $S$ of history records:

- $Sh(S) = \bigcup_{X \in S} Sh(X)$

#### Current (or latest) support, $Sc(X)$

- The *latest (metadata) support* of observation history record $X$, denoted $Sc(X)$, is the set of metadata history records defined as: For each record in $Sh(X)$, use the current latest record for that metadata item; equivalently, use the metadata *item* foreign key to retrieve that record from the primary table. 
- There is always exactly one of each metadata history record type (Network, Station, Station History, Variable) in this set.
- This set provides the *current state* of metadata relevant to $X$, with all updates to those items. This set *changes* when the corresponding metadata items are updated, and is not fixed over time.

We can easily generalize this to a set $S$ of history records:

- $Sc(S) = \bigcup_{X \in S} Sc(X)$

#### Support at tag, `St(X,T)`

Is this still relevant? It seems that if we define bookmarking as an association to a tuple of history records, then this whole thing is redundant.

A slightly less self-evident case of support

- The support set of of observation history record $X$ at tag `T`, denoted `St(X,T)`, is the set of metadata history records defined by: For each record in $Sh(X)$, use the metadata history record tagged by `T` for that metadata item. 
- There may be no such metadata history record for some or all of the elements of $Sc(X)$. Therefore `St(X,T)` may not contain one item for every metadata record type.
- Tag `T` can tag *any* metadata history record in an item's history set. Therefore the elements of `St(X,T)` may occur *before* the historical support items for $X$. This may or may not make sense in any given context.

It is possible to define other support sets with different criteria for what metadata history records are included, but defining the criteria so that they are consistent and make sense is harder. We do not offer any other definitions here.