# Introduction

## Essential references

The Alembic documentation is our fundamental reference. Note: The documentation appears only to be available for the latest version of Alembic (1.13 at time of writing), which is significantly ahead of the version in use in this project (1.6 atow). It must be read with this in mind; wherever things do not work as documented, this may be the answer. Examining the code may be a way to clarify issues.

Here is a list of key pages in the Alembic documentation:

- [Alembic documentation](https://alembic.sqlalchemy.org/en/latest/index.html)
  - [Alembic tutorial](https://alembic.sqlalchemy.org/en/latest/tutorial.html)
  - [Alembic cookbook](https://alembic.sqlalchemy.org/en/latest/cookbook.html)
  - [Operation reference](https://alembic.sqlalchemy.org/en/latest/ops.html)

## Existing Alembic infrastructure in project

When you have installed this project locally:

- Alembic is installed. You can use Alembic commands on the command line.
- The Alembic [migration environment](https://alembic.sqlalchemy.org/en/latest/tutorial.html#the-migration-environment) has been created and customized.
- Alembic-specific content is in the directory `pycds/alembic`, following standard Alembic practice.
- There are a number of already-written migration scripts in `pycds/alembic/versions`.
- Extensions to support non-table objects (e.g., functions, views, matviews) have already been written. 
  - Extensions to Alembic proper are in `pycds/alembic/extensions`.
  - Extensions to SQLAlchemy are in `pycds/sqlalchemy`. These support the Alembic extensions, although they can be and are used elsewhere too.

## Development activities

In the abstract, creating a migration is a relatively straightforward activity. This project, however, is fairly complicated, and writing migrations is correspondingly more complicated. 

In particular, managing non-table objects (functions, views, matviews, etc.) is not supported by Alembic out of the box. We have written extensions to support migrations involving them. Additionally, such objects are not mutable, and so must be dropped and replaced in their entirety -- unlike tables, in which elements (e.g., columns) can be modified in-place.

Such non-table objects are called here _replaceable objects_. 