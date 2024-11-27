# Version tagging use cases

We need a collection of use cases that encapsulate the intended usage of version tagging.
By "use" and "usage", we mean

> When we tag item X with version tag T, what does this mean in terms of "Give me data 
> for version tag T".

From these use cases we can develop an implementation that includes both table design 
and computations, the end result of which allows us to select data for version tag T. 
The present discussion will give us requirements for the implementation to satisfy, 
and remain agnostic about implementation details.

## Possibly useful definitions and terminology

### Database subset selected by tag

The basic question is "What observations and what associated metadata are selected by 
tag T?"

We will call this collection of data "subset T".

The question above is now rewritten: Given a tagging of records in the database with 
tag T, what is subset T?

### Metadata support of an observation

Observations are central to the utility of version tags, and to establishing rules and 
algorithms for processing tagging.

**Definition: Distance of a database (history) table from the Observation (history) 
table.**
1. Define the distance of the Observation (history) table as 0.
2. Define the distance of any metadata (history) table T from Observation (history) as 
   the maximum number of FKs that must be traversed to reach that table from 
   the Observation  (history) table.

| Table          |   Distance   |
|----------------|:------------:|
| StationHistory |      1       |
| Station        |      2       |
| Variable       |      1       |
| Network        | max(2,3) = 3 |


**Definition: _support_ of an observation P.** All those metadata records reached by 
following foreign key linkages in the direction of increasing distance from P. (This 
applies to both primary tables and history tables. The distances for each are the same.)

**Definition: records (of any type) _supported by_ a metadata record X.** All those 
records reached by traversing foreign key linkages in the direction of decreasing distance 
from observations. This is mainly of interest for supported observation records.


## Use case: Tag a set of observations with tag T. Later, retrieve subset T.

Steps: 
1. The user selects a set of observations in the database and indicates that they 
   are to be tagged T.
2. Later, potentially after many other operations on the database, user retrieves 
   subset T.
3. The user expects to access all and only those records associated by history keys to the
   tagged observations. See discussion below for a justification for this definition.

### Questions

**Q: What is subset T?**

The most obvious definition of subset T is:
- Those observations tagged T, and
- All metadata records supporting those observations.

**Q: What do we mean by "set of observations"?**

A1: By "set of observations", we might mean "any set of historical 
states of observations; that is, any set of observation history records".

It may be wise to confine ourselves to observation history records at the "same 
time", which might be a bit tricky to define properly, these definitions 
immediately suggest themselves:
- Within tolerance x of observation time t.
- Observations are all supported by the same set of metadata records, where "same" is 
  something like "from the same history state" ... so we would be looking at, for 
  example, observations supported by a set of StationHistory history records that were 
  all current at the time of tagging, and with one history record for any given SH item. 

Note: Restricting tag application to the latest state of any given observation would be 
simplest, but it may not meet requirements (e.g., when we want retroactively to tag 
some observation states). I think the "no more than one history record per supporting 
item" is the generalization of this case, and of the second definition of "same time".

Let's actually formalize that:

1. The _supporting metadata items_ are those metadata items identified by the 
   metadata history records supporting the observations.
2. For any given supporting metadata history item, no more than one of its history 
   records can be in the support of the observations.

A2: We likely do not want to tag two different historical states of the same
observation with the same tag. What would this mean? 

It would mean that the subset includes two historical  states of the observation, along 
with related metadata records for each observation. This is not inherently 
contradictory or wrong so long as we maintain the distinctions between those 
historical record sets. However it offers great potential for confusion.

Recommendation: Don't do this. Use distinct tags to tag distinct states of any given item.

**Q: Can subsequent updates those particular observations be considered part of subset T?
Which ones and why? What subsequent updates are excluded?**

The obvious answer for _observations_ is no. This is because tagging primarily 
targets the selection of _certain, specific historical states_ of an observation and its 
metadata support.

Therefore, no subsequent updates of any observation tagged T are included in subset T.

**Q: Can subsequent updates to the at-time-of-tagging metadata be considered part of 
subset T? Which ones and why? What subsequent updates are excluded?**

Okay, this is harder. 

Consider a more specific case:
1. Tag observation O with T.
2. Let SH be the station history record supporting O.
3. Update SH with a new location, giving history record SH'.
4. What is the location of observation O in subset T?

Supplementary questions:
1. Can SH' be considered in the support of O?
2. If yes, does it supersede SH? (Almost certainly, otherwise its inclusion in subset T 
   is pointless.)

This really comes down to, How much do we require tagging to "freeze" the state of the 
database?

Aside: We have a source of confusion here in that we have the existing and 
future-supported notion of the "latest" state of the database which takes no notice 
of tagging. (Right?)
And we have tagged states, which are more or less frozen.

**Fundamental question**: Why tag with a version? Answer: To enable the reconstruction of 
the database state at the time of tagging. So, for example:
1. Researcher R retrieves data at tag T, and performs an analysis. 
2. Updates (tagged or not? does that make a difference?) occur to some of those records.
3. Researcher S wants to verify R's analysis. They wish to retrieve the data at tag T.
4. If subsequent updates modify the set retrieved, then it is impossible to redo 
   the analysis with the original data.
5. Conclusion: Updates cannot be included in subset T.

**Summary**

1. No updates to metadata records or to observation records can be included in subset T.
2. Subset T consists of all and only those records associated by history keys to the 
   tagged observations.

**NB**: Tags perform two distinct purposes:
- Select a subset of the database (particularly, or centrally, observations).
- Select historical states at the time of tagging.
- See discussion of this below.


## Use case: Tag a (set of) metadata objects with tag T. Later, retrieve subset T.

Steps:
1. The user selects a set of metadata records in the database and causes them to be 
   tagged T.
2. Later, potentially after many other operations on the database, user retrieves
   subset T.
3. The user expects to access all and only those records associated by history keys to the
   tagged records. See discussion below for a justification for this definition.

### Questions

**Q: What is in subset T?**

The most obvious definition of subset T is:
- All those observations supported by the metadata.
- All those metadata records supporting those observations.

**Q: What do we mean by "set of metadata records"?**

A1: By "set of metadata records", we might mean "any set of historical
states of metadata records; that is, metadata history records".

As with observations, it may be wise to confine ourselves to metadata history 
records at the same "time", which is even trickier to define sensibly.

???:
- Project forward to observations.
- Project backward to metadata support.
- Take intersection with tagged records.

Restricting tag application to the latest state of any given metadata item would be
simplest, but it may not meet requirements (e.g., when we want retroactively to tag
some metadata states).

A2: We likely do not want to tag two different historical states of the same
observation with the same tag. What would this mean?

It would mean that the subset includes two historical  states of the observation, along
with related metadata records for each observation. This is not inherently
contradictory or wrong so long as we maintain the distinctions between those
historical record sets. However it offers great potential for confusion.

Recommendation: Don't do this. Use distinct tags to tag distinct states of any given item.

## Use case: Retrieve tagged observations with later metadata updates.

Motivation: A user wishes to retrieve a tagged set of observations but with 
important corrections (updates) applied afterward to some of the metadata supporting it.

Note: This is legitimate case not already covered: If we separate the tag from the 
object tagged, as we propose to do, then we cannot just say, "well after the update, 
tag the observations". (This would also be a heavyweight operation.) Instead we want 
to be able to update relevant metadata and ask for observations identified by a tag 
together with those updates. 

Consider the simplest case:
- One observation history record P
- Supported by one metadata history record M(t0)
- Item M is updated, yielding hx record M(t1)
- Retrieve P and M(t1)

If M(t1) is the latest update, then we are just saying, we want P plus the metadata 
item M at its latest state = M(t1). 

This is a straightforward query, since P includes (or implies) the item identifier for 
M, and we can easily select its latest state (history record).

The only complication arises if the user wants some intermediate (or past, even) history 
state between M(t0) and M(latest). The complication would be in specifying which one, 
since there can in general be an arbitrary number of updates to M after (or before) 
P was created.
- Is this even a likely scenario?
- It is not that hard to implement in queries if we choose the right parameters for 
  selecting M(t); for example a time period or a tag.


## Use case: Tag a metadata item; retrieve observations associated to earlier states?

This is a converse of the previous use case. But it's a little harder to define 
what observations would be included in this. Options:
- All observations supported by all states of the metadata item.
- The intersection of the above and a tagged or temporal selection of observations.




## Ancillary discussions

### Station history records: overlap with version history? (hint: no)

Station history records pose a minor puzzle because 
- the station history _items_ (as opposed to the version history records of the item)  
  form an existing user-facing history of the station, _and_ 
- each station history item itself has its own version control history

We may be best served by these notions:
- Distinct station history _items_ are due to **non-erroneous** updates of the station, 
  e.g., a real-world move of its location, not an error in entering its location there.
- History records for a fixed station history item are due to intended **corrections of 
  erroneous values in that item**, e.g., an incorrect entry of the station's location 
  for that station history item.

### Distinguish freezing from subsetting

Tags perform two distinct purposes:
- Freezing: Designate historical states of the database at the time of tagging.
- Subsetting: Designate a subset of the database records (in particular, observations).

Is it useful to separate these functions? Consider the following:

Freeze the database: All records are affected. This is like a git commit: The complete 
state of the database at the time of freeze-tagging is designated by the tag. This, 
implicitly or explicitly, applies the tag to all the latest history records 
throughout the database.

Subset the database: Only those records (observations only?) marked by the tag are 
included in the set designated by the tag.

Versioning is then the intersection of
- freeze the database at its current state (can we do past state?)
- subset those records that have been frozen

We may or may not want to distinguish separate _types_ (freeze, subset) of tags for this, 
rather than separate _effects_ for this.

Maybe this doesn't get us anywhere new, or perhaps it offers a simplification, via the 
notion of this intersection.

We do not have to create any additional data structures to support this concept. A 
simple algorithm suffices. Given a tag on any record:
- Select all history records at maximal distance from the tagged record.
- Select all history records associated to those maximal-distance records.

Effectively this looks like:
- Work back (increasing distance) to the Network history records associated to the tagged 
  record.
- Work forward (decreasing distance) to all history records associated to the Network 
  records selected above.
- The freeze set includes all associated metadata and observation history records 
  selected in the forward stage.


