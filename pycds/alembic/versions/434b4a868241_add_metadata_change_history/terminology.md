# Terminology

metadata collection
: A collection of _metadata items_. This term refers to both the
object that exposes the latest version of each metadata item and the history
table that contains the history of all metadata items in the collection.

metadata item
: A datum that contains metadata, identified by a stable _metadata id_.
A metadata item is mutable, that is, its existence and content can be changed
(created, updated, deleted) as well as read.

metadata item id (a.k.a. metadata id)
: A value that identifies a unique _metadata item_ within a _metadata collection_.
Typically an integer, drawn from a sequence.

metadata primary
: A database view or table object that is the primary user interface to a _metadata 
collection_. (It presents the latest version of each _metadata item_.)

metadata history table
: A database table that contains a record of every state of every _metadata item_
in a _collection_. The history table includes a _metadata history id_, which is unique
amongst all records in the table, and a _metadata item id_, which identifies a single 
metadata item and is common to all history records for that item. 

metadata history id
: A value that identifies a unique record within the entire _metadata history table_.
Typically an integer, drawn from a sequence.

reference
: Usage of a metadata id in a different collection. Parallel to a foreign key, but not 
the same, since FKs must be unique but metadata id's are common to all history records 
for the same item.

referencing collection
: A collection whose items refer (via a _metadata item id_ column) to a _metadata item_
in a different collection (the _referenced collection_). 
This is akin to a foreign key relationship from the referencing collection to the 
referenced collection. (For technical reasons it cannot be directly 
represented as such.)

referenced collection
: A _metadata collection_ whose _items_ are referred to (via a _metadata item id_ column) 
by items in a different collection (the _referencing collection_).
This is akin to a foreign key relationship from the referencing collection to the
referenced collection. (For technical reasons it cannot be directly
represented as such.)

# Naming conventions

metadata collection name
: Unrestricted.

metadata primary name
: Identical to metadata collection name

metadata history table name
: Determined by collection name: `<collection name>_hx`.

metadata item id name
: Independent of name of collection it is part of
: When used in a different (_referencing_) collection, the name must be the same 
as that used in the originating (_referenced_) collection. That is, the "home" name
must be used in all other collections.

metadata history id name
: Determined by collection name: `<collection name>_hx_id`.
: It is strongly recommended that the same name be used in all other tables
where it may be used (e.g., as a foreign key). That is, the "home" name
should be used in all other collections.

metadata history id sequence name
: Determined by collection name: `<collection name>_hx_id_seq`.



