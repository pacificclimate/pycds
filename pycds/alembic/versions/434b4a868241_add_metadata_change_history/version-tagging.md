# Version tagging

The input we received from the CRMP coordinator indicates that we need a feature by which
an observation -- and very likely associated metadata -- can be tagged. In this model,
an observation can be issued in multiple "versions". Let's clarify what we mean here:

## Terminology

Observation
- A numerical value primarily identified by spatial location, time, and variable.
  For example, "the temperature (variable) at YYJ (location) at 10:00 am on Jun 6,
  2020 (time)".
- In the CRMP database, the primary information is provided via the metadata
  associated to each observation, plus a time value stored directly in the
  metadata table.
- The direct metadata associations are to:
    - station history, which provides location
    - variable
- Indirect metadata associations provide other information about the observation, such
  as the id of the station it was recorded at, or the network providing the data.

Version tag
- A unique identifier applied to a record (observation or metadata) that 
  identifies it as being part of a versioned dataset.
- An observation for given place, time and variable may be issued repeatedly, with,
  potentially, a different observation value as a result of QA processes or other
  processing after the "raw" value is recorded from the sensor.
- Any single such value for the same place, time, variable is called a version.
- A version has a name or identifier that distinguishes it from other versions.

## Scenarios

1. CRMP partner releases a new version of a dataset. 
   1. A version implies updates to some of the observation values. 
   2. Can it include changes to metadata? (If so, what kind of 
      changes are possible?) crmprtd will give us significant information on this.
2. PCIC modifies some subset of the observations. This must be distinguished from  
   modifications due to a version released by a partner.
3. PCIC modifies one or more metadata items (e.g., update a network name, modify the 
   locations of several stations). This must be distinguished from modifications 
   due to a version released by a partner, particularly if metadata can be modified 
   by a partner.


## Proposal A

Every table, observation and metadata, carries a version.

## Questions

What does it mean to apply a version tag?

JS: It should be like a git commit. Implicitly or explicitly, applying a version tag 
is an operation on the whole database. (Or a subset of it? Because versioning is in 
part and maybe in whole an operation to distinguish a subset of one network's 
observations.)

RG's original idea: Just another change to some table, becomes part of history.

Is there a notion of scope of tagging that must be applied here? I.e., that tagging 
applies to specified subsets? Pretty much yes. How to represent? What does this imply 
for database queries?

How about: A version tag selects a subset of the database. That subset is determined 
by the relationships between the tables. What are the cases and rules for this?

1. Tag a subset of observations. This has the following implicit (but likely not explicit)
effect:
   1. The observations in question are tagged. 
   2. The metadata applying to the observations in question are tagged.
2. Tag a metadata item. Effect:
    1. ???
2. Let's get more explicit: Tag a set of (updated) variable items. What else should 
   be tagged in this operation?
   1. Associated observations.
   2. Upstream metadata.

This seems to imply that tagging applies both upstream and downstream of the item 
tagged. This is akin to the git commit model, but restricted to the subset of records 
selected by associations within the database.




## Old stuff


By "tagging an observation", we mean associating a datum (the "tag") with an observation.

Since observations have a history, and tagging i

If we continue the design proposed above, then we need tag only observations. All
associated objects (metadata) are tagged by association, or more to the point, the FK
relationships between history tables preserve the metadata state at the time
the observation record is tagged, i.e., for a given observation history record.

Moreover, we can also easily retroactively tag earlier history items. This is most
cleanly done with a many:many tagging design (see below), since the tag association is
not stored in the observation history record.

Design decision: What is the relationship between observation records and tags?
- many:1 (one tag per observation)
- many:many (many tags per observation)

This decision affects the table design and the queries necessary to answer questions
such as "What observations are tagged X?" But it does not affect fundamentals such as
what kinds of questions can be answered.  

