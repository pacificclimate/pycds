# Bookmarks aka version tags aka commits aka ...

## Table of contents

  - [Facts and assumptions](#facts-and-assumptions)
  - [Terminology](#terminology)
  - [Basic operations](#basic-operations)
    - [Note on history tuple vs `mod_time` for bookmarking](#note-on-history-tuple-vs-mod_time-for-bookmarking)
    - [Validate a history tuple](#validate-a-history-tuple)
    - [Get the latest undeleted (LU) records, given a subset of history records](#get-the-latest-undeleted-lu-records-given-a-subset-of-history-records)
    - [Create a bookmark label](#create-a-bookmark-label)
    - [Bookmark a point in history (create a bookmark association)](#bookmark-a-point-in-history-create-a-bookmark-association)
    - [Bracket (or group) a set of updates](#bracket-or-group-a-set-of-updates)
  - [Applications of bookmarking](#applications-of-bookmarking)
    - [Efficient grouping of data versions](#efficient-grouping-of-data-versions)
      - [An unsatisfactory approach](#an-unsatisfactory-approach)
      - [Bracketing](#bracketing)
    - [Historical reconstruction or Rollback](#historical-reconstruction-or-rollback)
      - [Outline](#outline)
      - [Implementation considerations](#implementation-considerations)
    - [Extraction of specific subsets](#extraction-of-specific-subsets)
      - [Example 1: Records inserted or updated within a single bracketing](#example-1-records-inserted-or-updated-within-a-single-bracketing)
      - [Example 2: Records inserted or updated in a specific time period](#example-2-records-inserted-or-updated-in-a-specific-time-period)
  - [Implementation notes](#implementation-notes)
    - [Tables](#tables)
  - [Other discussions](#other-discussions)
    - [Updates to existing records: what supporting metadata?](#updates-to-existing-records-what-supporting-metadata)
    - [Metadata support](#metadata-support)

_TOC courtesy of [Lucio Paiva](https://luciopaiva.com/markdown-toc/)._

## Facts and assumptions

**Facts**

- History tables are append-only. 
- Each history table records the changes made to the entire collection *in temporal order of the changes*. 
- Each successive update to a collection is recorded by appending a record to its history table; therefore temporal order is also the order by ascending history id. 

**Assumptions**

- No existing record in a history table is ever modified.

**Therefore**

- If a bookmark is associated to a record in a history table, it represents the history of that collection up to that point in time. A bookmark association can be thought of by analogy with a Git tag, in the sense that both are pointers to a specific state of the relevant items.
- Two such bookmark associations, say $B_1$ and $B_2$, bracket a set of changes recorded in the history table. The delta between them is exactly those changes recorded in the history table, in history id order, between $B_1$ (exclusive) and $B_2$ (inclusive).

**For further consideration**

- Bookmark associations can be, and most naturally are, stored in order of the association operations, that is, temporally. Therefore we can read out a series of successive changesets simply by examining the bookmark associations in the order they are made. 
	- However, that is not true if we allow bookmarking of non-latest states, which is probably going to be needed. We already have history, and we won't always anticipate future needs. Hmmm.
	- Alternative ordering for bookmarks: In the order they occur according to the history table id, that is in history table temporal order. These should be consistent across all history tables; verify this thinking.

## Terminology

- **Bookmark**: A named object that designates a *point in history*. This is an imprecise usage of the term "bookmark", which is actually two related things, a *bookmark label* and a *bookmark association*:

- **Bookmark label**: A data object bearing a label used for bookmarking. Several different points in history can be tagged with the same label. This allows a common label to be used to group multiple items.

- **Bookmark association**: A data object that associates a *bookmark label* to a specific point in history. This is the fundamental operation of bookmarking.

- **Point in history**: The current values of all items in history-tracked tables. It excludes non-history tracked tables, except as adjuncts to the current state. (Note that we cannot be sure what the non-history tracked tables might have contained in the past. Only the present is knowable with such tables.)
	- **Current point in history**: The state of the history-tracked collections at the current point in time, in whatever sense "current" can be understood.
	- **Past point in history**: An actual previous state of history-tracked collections. Such a state was  the current point in history in the database at some moment, however briefly.
	
- **History tuple**: A tuple of history id's, one for each history table, in some specified order of those tables. There are many possible tuples of history id's, but only some of them represent actual points in history. Those that do represent actual points in history are called (valid) history tuples. The rest are invalid and represent nothing useful. See also *validity*, below.

- **Historical subset**: A history tuple defines a subset of the history, namely all those history records in each history table that occur at or before the corresponding history id in the tuple. Under the assumption (see above) that the temporal order of history records is the same as the history id order, "at or before"  means that history id is less than or equal to the history id in the tuple. See also *validity*, below.

- **Validity** (of historical subset, of history tuple): 
	- A historical subset is valid if and only if it exhibits referential integrity. That is, all references by a history record in that subset to another history record must also be found in the subset. Otherwise (i.e., when there is a violation of referential integrity within the subset) the historical subset is not valid.
	- A history tuple is valid if and only if it implies a valid database historical subset.

- **Latest undeleted (LU) records**: Given a set $H$ of history records drawn from a single history table:
	- Informally:  The undeleted (LU) records are the latest (most recent) history records in that set, one for each item (distinguished by item id) not marked as deleted within the set.
	- Formally: Define $LU(H) \subseteq H$ such that:
		- For each item id $i$ present in $H$ (recall that item id's are not unique in history records):
			- there is at most one history record in $h_i \in LU(H)$ with item id $i$;
			- $h_i$ has the largest history id of all history records in $H$ with item id $i$;
			- that item has not been deleted ($h_i$ does not have the deleted flag set).
	- If $H$ contains all records in a history table prior to some point, the LU set represents what that ollection looked like at that point in time.
	- Given a *valid historical subset*, then the collections of LU records from each history table in the subset give us the state of the history-tracked collections at the point in time represented by the historical subset.
## Basic operations

### Note on history tuple vs `mod_time` for bookmarking

We could, in principle, use `mod_time` to define a point in history. A history subset would then be just all history records occurring at or before a given `mod_time`.  This would have some advantages:

- A single value rather than a tuple denotes a point in history.
- Validity (referential integrity) of a historical subset is trivially guaranteed so long as all history records satisfying the `mod_time` condition are included in the subset. There's no need to validate such a time value as there is for a tuple.

However, we cannot always trust times in the database. Rather than fretting over time synchrony, we use the more reliable marker of position that is the history id. In that case, we have to store the history id's for each history table; hence a history tuple, not a single time value.

***Note***: If all history tables shared the same history id sequence, then a single history id would in fact point unambiguously at a point in history, in the same way as `mod_time` but without the doubts about time. This might be worth considering. Notes:

- You'd have to obtain from the shared history id sequence (for current point in history) or the history tables (for past points) the largest history id corresponding to your desired point in history.
- You'd need to migrate all the existing tables. For `obs_raw_hx`, this would be a pretty big operation, again, but nothing like as big an operation as the original history table population.

### Validate a history tuple

$Validate(S)$: Given a history tuple $S$, check that it is valid. Raise an error if it is not.

1. The naive algorithm checks every reference in any given subset for presence in the referenced collection historical subset. But this is a huge number of records in most cases.
2. A less naive and much faster algorithm relies on the assumption that every actually occurring historical state of the database was valid (quite a reasonable assumption!), and therefore that history tables reflect that. This assumption allows us to check only that reference history id's are less than or equal to the corresponding collection history id in the tuple.

### Get the latest undeleted (LU) records, given a subset of history records

It's straightforward to construct a query that yields the history id's of the LU items in a given collection. These history id's can then be used to extract part or all of the LU records from the history table. 

Here's a specific example for `meta_network_hx`. In this query, `<condition>`  is the condition that extracts the subset from the full history table. We can generalize this query to any history table. 

```
SELECT  
    network_id,  
    max(meta_network_hx_id) AS max_hx_id  
FROM  
    meta_network_hx  
WHERE <condition>  
GROUP BY  
    network_id  
HAVING  
    NOT bool_or(deleted)  
```

The `<condition>` could look like one of the following:

- `meta_network_hx_id <= upper_bound` (full previous point in history)
- `lower_bound < meta_network_hx_id AND meta_network_hx_id <= upper_bound` (partial history between two previous points)
- `mod_time <= upper_bound` (full previous history bounded by when modifications were applied)
- etc.

Again looking a little forward in this document, a common case will be where the condition is related to one or more bookmarks. 
##### Notes

- Fidelity to actual history would require that there are no gaps in the set of history records, i.e., that we haven't arbitrarily dropped some from the middle. However, that is not strictly necessary for these operations to be performed.)

### Create a bookmark label

**Motivation**: A bookmark label is an object used to tag one or more points in history. It is independent of the point(s) in history it is associated to.

**Operation**: The act of creating a new bookmark label $L$, eliding the details of bookmark implementation, is denoted $L = CreateBookmarkLabel(N, ...)$ where $N, ...$ is the bookmark name and other (elided) details.

### Bookmark a point in history (create a bookmark association)

**Motivation**: This is the fundamental operation in bookmarking. 

**Operation**: Let $L$ be a bookmark label. Let $S$ be a tuple of history id's. Then, eliding the details of the association data object:
- $B = Bookmark(L, S)$ denotes the atomic (transaction enclosed) operation of:
	- validating $S$, and then
	- associating bookmark label $L$ to state $S$
	- returning the bookmark association $B$.
- The shorthand $Bookmark(L)$ is defined as $Bookmark(L, S)$ where $S$ is the current state of the database.

***Note***: For information on validating a tuple of history id's, see [[#History tuples, historical subsets, and their validity]].
### Bracket (or group) a set of updates

**Motivation/scenario**: A set of related updates are received or made all at one time. The canonical case is a QA update of a large set of observations. (In other cases, e.g., when a scientist is updating things, it will take a certain amount of discipline to make sure that the updates are batched together like this.)

**Operation**: Let $L_1$ and $L_2$ be two bookmarks. Let $U$ be a set of updates. Then the operation  $Bracket(L_1, U, L_2)$ is defined as:

- *Within a transaction (i.e., in isolation from other operations)*: 
	- Perform $Bookmark(L_1)$.
	- Perform updates $U$.
	- Perform $Bookmark(L_2)$.

History records between $L_1$ (exclusive) and $L_2$ (inclusive) are exactly and only those changes made in the updates. This is due to: 

- their isolation in the transaction (so no other update operations are interleaved); 
- the fact that change records are appended to the history tables in temporal order of change operations.

**Other notes**:

- Since the bookmarks for bracketing are directly related, we would probably do better to use a single bookmark label $L$, and allow the system to construct bookmarks $L_1$ and $L_2$ from $L$. In fact, we use the same label, and the bookmark association carries the distinction between  $L_1$ and $L_2$.  We can then define  $Bracket(L, U) = Bracket(L_1, U, L_2)$, where $L_1$ and $L_2$ are the constructed bookmarks. See use of auxiliary columns in bookmark association table in [[#Implementation notes]] below.

**Transactions, concurrency, and transaction isolation level**

Bracketing is useful only if operations occurring outside the transaction in which bracketing is performed cannot be interleaved with the operations occurring inside it. This is called transaction isolation.

The Postgres documentation for [Transaction Isolation](https://www.postgresql.org/docs/current/transaction-iso.html) states:

> The SQL standard defines four levels of transaction isolation. The most strict is Serializable, which is defined by the standard in a paragraph which says that any concurrent execution of a set of Serializable transactions is guaranteed to produce the same effect as running them one at a time in some order. The other three levels are defined in terms of phenomena, resulting from interaction between concurrent transactions, which must not occur at each level. The standard notes that due to the definition of Serializable, none of these phenomena are possible at that level. (This is hardly surprising -- if the effect of the transactions must be consistent with having been run one at a time, how could you see any phenomena caused by interactions?)

Therefore not only must we run bracketing within a transaction, the transaction must be run at Serializable isolation level. This is done with the [`SET TRANSACTION`](https://www.postgresql.org/docs/current/sql-set-transaction.html) command within the transaction in question.

## Applications of bookmarking

### Efficient grouping of data versions

CRMP partners periodically release new versions of a dataset. Such releases typically contain many thousands of observations. A version release amounts to an update to each such datum. 

Releases typically are, in time order: 

1. A raw dataset. This dataset frequently arrives incrementally, via `crmprtd`.
2. A QA-adjusted dataset. This dataset is expected to arrive in one or a few large batches.

Each observation in the second release is a revision to an observation in the first release.
#### An unsatisfactory approach

It is possible bookmark observations individually, by associating a bookmark to each updated item's history records. This requires the same number of bookmark associations as there are observations, i.e., it scales linearly with the number of observations. Given the number of observations, this is highly undesirable and should be avoided whenever possible.
#### Bracketing

Bracketing uses only two bookmarks per group; one each to demarcate the beginning and end of a group:

- Let $U$ be the set of updates that update the database for a given release.
- Define bookmarks $L_1$ and $L_2$ to demarcate (bracket) the release. Below, this amounts to using bracket-begin and bracket-end in the association of the same bookmark label.
- Perform $Bracket(L_1, U, L_2)$.

Bracketing requires only constant time and space relative to the number of updates within it.

##### Bracketing for large datasets (QA releases, other updates)

QA releases and other updates are expected to arrive in large batches. If it is not operationally possible to perform the updates for a new release within a single transaction, this approach can generalized to a small number of bracketing operations which together encompass the whole release. This will still significantly reduce the space and time required to bracket and retrieve a large group of observations, even if it does scale linearly. (A 3+ order of magnitude reduction in associations is still a significant win.)

##### Bracketing for regular ingestion (`crmprtd`)

Regular ingestion (via `crmprtd` and related scripts) occurs piecemeal. Typically, dozens to thousands of observations are ingested at a time (hourly, daily, weekly, or monthly, depending on the network). We can use bracketing for each such group of observations ingested. This is still much smaller than a typical QA release, and it does scale linearly in total observations, but it is nonetheless much better than bookmarking one observation at a time.

##### Considerations

Bookmarks used to bracket each ingestion should bear a clear and easily queried relationship to each other. This enables an entire dataset (e.g., raw, QA'd) to be extracted with simple, error-resistant queries. The design of bookmark associations, specifically columns `role` and `bracket_begin_id`, support this directly.

### Historical reconstruction or Rollback

#### Outline

The design of history tracking makes it easy (although not necessarily *fast*) to reconstruct a point in history from a bookmark (or more precisely a bookmark association). In other terms, to produce the historical subset given a history tuple.

The definition of LU (latest updated) records provides the necessary tool. Rollback is just the operation of generating LU records and storing them somewhere.

#### Implementation considerations

For metadata tables, which have few records, the basic LU query is likely fast. 

For `obs_raw_hx`, the LU query will necessarily scan a huge number of records. To make it perform better:

- Create appropriate indexing on the history tables.
- Use the tightest possible WHERE conditions. If, for example, only observations from a specific network or from specific stations are desired, then encode that in the WHERE condition.

For efficiency and convenience we are likely to want to store the result in a separate set of tables, which are best housed in their own separate schema.

In a separate schema, call it `crmp_rollback`, do the following. Given a bookmark:

- Establish structural copy of the `crmp` schema (i.e., table definitions without data) including main tables but excluding history tables (history tables are redundant here).
- Include FK relationships, indexes, and other things as needed.
- Duplicate the content of the non-history tracked tables (at the time of of this rollback).
- Populate the history-tracked tables using queries as above.
- Define and populate (one row) a rollback table that contains at least the following information:
	- bookmark association ids, if relevant 
	- text of WHERE condition ... the bookmark association ids may not be relevant or the full story
	- timestamp when this rollback was established
	- id of user creating the rollback
	- any other important information not retrievable with this data
- Possibly make all tables read-only. 
- Possibly make this schema accessible only by a specific role which is granted only to the user(s) who need this version of the database.

And, in the main `crmp` schema, define a stored procedure that does all of the above:

- Given a bookmark id (or more broadly a where condition) and rollback schema name
- In a valid order (respecting foreign key dependencies)
- With optimized queries insofar as possible 

Once the rollback schema is populated with data reflecting a given point in history, the users with interest in it can query it as if it is the actual CRMP database. It will not in general be wise to allow the users to modify this database, since those modifications will not under any circumstances be propagated to the real database. For experimental purposes, with appropriate, loudly stated caveats, this rule may be relaxed.

Any number of rollback schemas can be established. They do not interact with each other, nor with the live CRMP database. But because each rollback schema will be comparable in size to the live database, we may wish to limit their number and their lifetimes.

### Extraction of specific subsets

In some cases, it's possible that the (historical) records of interest lie only in a restricted range. This would be a much smaller dataset than the entire set of records in `obs_raw` at a given point in history. With suitable indexes, these subsets will be much faster to extract and work with than the whole of the history-tracked tables. Some examples:

- The data selected by a particular bookmark. This might be several groups or just one, depending on how the bookmark was used -- for example, depending on whether it bracketed just one group of data or several.
- The data selected by a particular date range. This would require extracting the (latest) historical records for that time period. This does not involve bookmarks.
- A combination of the above.

#### Example 1: Records inserted or updated within a single bracketing

This could correspond to a QA update to a previously ingested set of raw observations.

Let `b_begin` and `b_end` represent tuples either externally provided history tuples (substituted in to the query), or tuples queried directly from the bookmark tables, corresponding to a bracket-begin and bracket-end pair.

The bracketed set of updates may include multiple updates to a single item in a given collection. After the fact, we are only interested in the final outcome, which is the *latest* value for each collection item in the bracket. We must do a little extra work to obtain the latest indexes, which means taking the latest (equivalently, greatest) history id within the subset for each item id.

See [[#Given a set of history records, get the latest undeleted (LU) records]].

##### Caution! Bracketed sets vs valid historical subsets

It's tempting to think that the collection of records selected from each history-tracked collection by the above process would jointly constitute a valid historical subset. Not so! 

Let's consider what is probably the most common and germane example, updates to `obs_raw`. 

Suppose the bracketed updates were *only* to `obs_raw`, a not unlikely scenario. Then the collection of history records obtained by the above process, from all history tables, contains only records from `obs_raw_hx`. There are no metadata records whatsoever in this set, because none were modified. But those `obs_raw_hx` records necessarily point at metadata history records ... which are not in the set. 

What's going on here? The historical metadata supporting the observations is drawn from the entire set of latest records prior to the bracket-end bookmark, potentially as far back as the first record ever inserted.

It's important to keep this slightly subtle point in mind when working with brackets or other historical subsetting.

##### Question: Which supporting metadata?

Again, considering brackets with updates only to `obs_raw` will bring things into sharper focus.

We have three different plausible choices for the metadata supporting these observations:

1. The historical metadata directly linked to each `obs_raw_hx` record within the bracket.
2. The *latest* version, *within the subset implied by the bracket-end*, of the metadata linked to each `obs_raw_hx`.
3. The *current* version of the metadata item linked to each `obs_raw_hx` record within the bracket. This is not constrained by the bracket-end. Therefore, unlike the above two records, it can vary as time passes, i.e., as further updates to the linked records are made. Those updates can include deletion, so it is doubly perilous to consider using this choice. We cannot recommend it.

The correct choice depends on context and intention, although (3) is highly questionable. There is no universally correct choice.
#### Example 2: Records inserted or updated in a specific time period

We can also form a subset based on time constraints. This is not fundamentally different, but there are some additional or sharpened considerations.

Let `t_begin` and `t_end` be timestamps defining the time period of interest. We want to extract the subset of records that were inserted or updated in this period.

See [[#Given a set of history records, get the latest undeleted (LU) records]].

All three considerations described above about what records are germane in the subset are important here:

1. The process of obtaining the latest historical value for each item selected within the time period. Even more so than with a bracket, the collection items selected within a specific time period may experience multiple updates.
2. Time period constraints do not provide any guarantee that the history records associated to any within the temporal subset are also within that subset. And still less if the time constraints are selected for one particular collection and not with all collections in mind.
	1. [[#Caution! Bracketed sets vs valid historical subsets]] is relevant, but with the time constraints playing the role of the brackets. This is perhaps even more pointed because of the lack of the sanity guaranteed by a bookmark's validity constraint.
	2. [[#Question Which supporting metadata?]] This translates over pretty much unchanged.

## Implementation notes

A provisional implementation is on branch [i-239-version-tagging](https://github.com/pacificclimate/pycds/tree/i-239-version-tagging). It includes types, tables, functions, trigger functions. In this section we just summarize and make some remarks and questions. 

### Tables 

**Table `bookmark_labels`**

Questions:

1. Apply history tracking to this table? Reason, utility?
2. FK `network_id` 
	1. Is it actually needed? A network is implied by a bookmark's association to items. This is also true of variables, but they too have a indirect network association (`network_id`) that is not strictly necessary. That fact inspired the idea of having a direct association for bookmarks as well. This over-specificity (really: denormalization) may be offset by the utility of easily segregating these things (variables, bookmarks) by network, and establishing their relationship to network *before* use elsewhere.
	2. Nullable? Tempting, but caution, nullable columns have frequently been abused in CRMP.

**Table `bookmark_associations`**

Q (rhetorical): Why separate association from bookmark proper? 
A: To support multiple uses of the same bookmark.
- Brackets share the same bookmark info, but are associated as bracket-begin, bracket-end. 
- We likely want to bracket multiple groups of observations -- e.g., those ingested by `crmprtd` at any one time -- using the same bookmark.

Questions:

1. Apply history tracking to this table? Reason, utility?

Constraints on bookmark associations and role:

- Singleton bookmarks are permitted in any pattern, no constraints except tuple validity.
- We allow any pattern of bracket bookmark associations: disjoint, nested, overlapping. This is because we have no current knowledge of what patterns will be useful in future, and no logical reasons to exclude any. 
- The only constraints on brackets are:
	- bracket-begin and bracket-end must occur in matching pairs (open brackets, i.e., unmatched bracket-begins, are permitted).
	- a bracket-end must specify an open (not yet paired) bracket-begin that occurs before (in order of ascending `bookmark_association_id`) the bracket-begin.
- We can (see trigger function): 
	- Auto-generate `bracket_begin_id` for a bracket-end id when there is only one unpaired bracket-begin (that one's bracket id). This seems a likely scenario. However, it is surplus to requirements if we assume/require the user to carry the auto-generated bracket-begin id.


## Other discussions

### Updates to existing records: what supporting metadata?

This isn't necessarily a question about bookmarking, or isn't exclusively so.

Let's use `obs_raw` as the example, as it is covers all other cases. 

When we update an existing `obs_raw` record, we can ask what are the relevant metadata history items to link with it in history. 

This question is interesting because now that we have history tracking we also have a new phenomenon, which is that updates to metadata and updates to observations are not interchangeable. Order now matters.

For any given `obs_raw` record, possible answers are:

1. The current (latest) metadata history record for the metadata item linked to by the `obs_raw` record.
    - The simplest answer, and the one that matches what happened before the advent of history tracking. It is also what happens automatically via the history foreign key maintenance trigger whenever a record is created or updated. 
2. The metadata history record linked to the existing (un-updated) `obs_raw_hx` history record for that item (this may be the same as the current one, but it equally well may be quite a lot older).
    - It is possible to imagine wanting to update a "frozen" state of the database, i.e., to update `obs_raw` based on an earlier state of the database that could be obtained from a history rollback, which would imply using the links to the older states of the relevant metadata items.
3. Some intermediate metadata history record between those for (1) and (2), if any exist.
    - This just seems like asking for trouble. 

There is no *a priori* answer to this question; any answer could be correct. To multiply questions and confusion, this choice applies transitively to all metadata items linked indirectly to the obs_raw record (i.e., station and network).

### Metadata support

This a closely related topic, and maybe can precede the discussion above, but ...

The *historical metadata support* of an observation history record $H$ is the set of metadata (history) records directly relevant to $H$, which is to say directly associated to $H$ by one or more FK links away from the observation. 
We denote this by $S(H)$. Notes:

- This in fact applies to any history record $H$, but observation histories are the most important and are the most general or complex case.
- We have only unidirectional links to consider at the moment, but if we include many:many relationships in history tracking, then this definition will possibly become a little more complicated.

The historical metadata support of a set of observation history records $K$ is the union of the support of each record $H \in K$. We write $S(K) = \bigcup_{H \in S} S(H)$.

When we select a set of observations to be bookmarked after the fact, that is after the database has experienced more changes, we need to also include the metadata support of that set. This will make bookmarking after the fact somewhat trickier.

As we note above, we can think about the *current metadata support* for history records, which would be those records linked via item id, not by history id.

This is really just a restatement of the considerations above.





