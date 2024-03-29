# IMPORTANT ADVICE AND GUIDELINES

## Table of contents

- [Introduction](#introduction)
- [Order of development activity](#order-of-development-activity)
- [Multiple versions of the same replaceable object](#multiple-versions-of-the-same-replaceable-object)
   - [Locations of versions of replaceable objects](#locations-of-versions-of-replaceable-objects)
- [Obtaining the correct version of replaceable objects in migrations](#obtaining-the-correct-version-of-replaceable-objects-in-migrations)
- [Creating functions (stored procedures) using untrusted languages](#creating-functions-stored-procedures-using-untrusted-languages)

_TOC courtesy of [Lucio Paiva](https://luciopaiva.com/markdown-toc/)._

## Introduction

As noted elsewhere, creating a migration is not in principle very complicated. But in this project, where we manage many non-table objects, there are complications. We try to document those complications here.

## Order of development activity

1. Replaceable objects require multiple versions to be retained under the revision identifier (see notes below). So the first step is usually to create a new skeleton migration from which the revision identifier can be obtained.

2. Modify the ORM to reflect the changes to be implemented. This can include:

   - Modifying a table (e.g., column definitions)
   - Creating a new version of a replaceable object to be created or updated (e.g., a function, view or matview)

3. Complete the migration script using the newly defined objects. See the next sections for some important advice on doing that.

## Multiple versions of the same replaceable object

For each migration script to work in perpetuity without error, **_all_** versions of replaceable objects must be retained in the codebase. A given migration will use only one or perhaps two distinct versions of a replaceable object, but there may be many such versions corresponding to many revisions (and attendant migrations). 

For example, a view or materialized view may acquire new columns and have the query defining its columns updated accordingly. But because earlier migrations involving that matview depend on earlier versions of that object, those versions of the matview must also be retained.

### Locations of versions of replaceable objects

All object definitions are defined in the module `pycds.orm`. This module is further subdivided into object types such as tables (`pycds.orm.tables`), views (`pycds.orm.views`), etc. Whereas tables are mutated and do not need to have old versions retained, replaceable objects require all versions to be retained separately. Within each replaceable object type module (e.g., `pycds.orm.views`), there are separate version modules named according to the version in which one or more replaceable objects in that version is (re)defined (e.g., `pycds.orm.views.version_bb2a222a1d4a`).

(This organization is a little awkward in practice, but logical. Other and possibly better ways of organizing things are conceivable.)

## Obtaining the correct version of replaceable objects in migrations

Recall that a replaceable object (a.k.a. non-table object) is one that cannot be modified in-place in the way that a table can. Instead (prompting the name), such an object must be replaced in its entirety.

**In a migration script that updates a replaceable object, it is _essential_ to obtain the old and new replaceable objects directly from the version directories where they are defined.** This ensures that the correct version is dropped and/or created. 

When dependent objects must be dropped and recreated in the course of updating an primary object, ensure you get each dependent object from the correct version directory, and not from the top-level `pycds` module. If you do not do this, migrations will fail or potentially update the database with the wrong objects.

## Creating functions (stored procedures) using untrusted languages

In order to perform certain operations, the migration user needs superuser privileges. These operations include:

- Create a stored procedure using an untrusted language (we use `plpython3u` for some) 
- Create or drop an extension. (This should be rare, since extensions are not generally under control of the ORM.)

We do not give high privilege to users unless they require it. In migrations, we get superuser privilege by temporarily setting the role to a superuser role name that is granted to the user only for the period when database migrations are performed. After those operations have been performed, the superuser role should be reset. For example:

```python
    op.set_role(get_su_role_name())
    op.create_replaceable_object(function_in_untrusted_language)
    op.reset_role()
```
