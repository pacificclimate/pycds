# PyCDS

![Python CI](https://github.com/pacificclimate/pycds/workflows/Python%20CI/badge.svg)
![Pypi Publishing](https://github.com/pacificclimate/pycds/workflows/Pypi%20Publishing/badge.svg)

PyCDS is a Python package that provides an
[Object Relational Mapping (ORM)](http://en.wikipedia.org/wiki/Object-relational_mapping)
layer for accessing meteorological observations stored in a relational database
in a standard database model, referred to variously as a CRMP database or a PCDS database.

This package also uses [Alembic](https://alembic.sqlalchemy.org/) to manage database creation and migration
(see section below).

This type of database (i.e., with the PCDS/CRMP model) is currently being used at PCIC to
store BC's long-term weather archive, the Provincial Climate Data Set (PCDS), and
the Northern Climate Database (dbnorth). For details, see Background below.

With this package, one can recreate the database schema in [PostgreSQL](http://www.postgresql.org) or
[SQLite](http://www.sqlite.org)
and/or use the package as an object mapper for programmatic database access.
PyCDS uses [SQLAlchemy](http://www.sqlalchemy.org) to provide the ORM layer.

## Installation

One can install PyCDS using the standard methods of any other Python package.

1. Clone our repository and run the setup script

    ```
    $ git clone https://github.com/pacificclimate/pycds
    $ cd pycds
    $ python setup.py install
    ```

2. Or just point `pip` to our `GitHub repo](https://github.com/pacificclimate/pycds)

    ```
    $ pip install git+https://github.com/pacificclimate/pycds
    ```

## Background

### Provincial Climate Data Set (PCDS)

The Provincial Climate Data Set (PCDS), is a collaboration between the BC Government's
[Climate Related Monitoring Program (CRMP)](https://www2.gov.bc.ca/gov/content/environment/research-monitoring-reporting/monitoring/climate-related-monitoring)
and the
[Pacific Climate Impacts Consortium (PCIC)](http://www.pacificclimate.org/).

PCDS is an archive of meteorological observations for BC collected by federal agencies,
BC ministries and crown corporations and dating back to the late 1800's. The archive consists of a relational database
that models the data collected by multiple agencies (each of which are represented by one or more "networks" in the
database) at multiple locations (represented by "stations").

### Climate Related Monitoring Program (CRMP)

The [Climate Related Monitoring Program (CRMP)](https://www2.gov.bc.ca/gov/content/environment/research-monitoring-reporting/monitoring/climate-related-monitoring)
is a collaborative effort between several BC ministries, crown corporations, and PCIC.
Its purpose is to opportunistically leverage weather observations that are being collected for operational uses and
utilize them for long-term climate monitoring.

## ORM contents and usage

PyCDS defines the following contents of a CRMP/PCDS database:

- schema name
- tables
- stored procedures
- views
- materialized views


### Schema name

All contents of a PyCDS database are defined in a named schema.

SQLAlchemy documentation [recommends](https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#remote-schema-table-introspection-and-postgresql-search-path)
against using the `search_path` to establish schema name:

> keep the `search_path` variable set to its default of `public`, name schemas other than `public`
explicitly within `Table` definitions.

Unfortunately, SQLAlchemy does not make it easy to set the schema name at run-time.
Since SQLAlchemy ORMs are defined declaratively, not procedurally, the schema name must be determined at
"declare time" (i.e., when SQLAlchemy table classes are processed), which is done most conveniently
by fetching the name from an environment variable. Once classes are declared, the schema name
cannot be (re)set.

**The schema name is specified by the environment variable `PYCDS_SCHEMA_NAME`.**

If `PYCDS_SCHEMA_NAME` is not specified, the default value of `crmp` is used,
making this backward compatible with all existing code. The function
`pycds.get_schema_name()` returns this value.

**IMPORTANT:** `PYCDS_SCHEMA_NAME` must agree with the name of the schema targeted in
an existing database, or with the intended name for the creation of a new database.

In the case of an existing database, a mismatch between `PYCDS_SCHEMA_NAME` and the
actual database schema name will cause operations to
fail with errors of the form "could not find object X in schema Y".

### Tables

The tables defined in PyCDS are all those found in a standard CRMP database.

Tables are defined using the SQLAlchemy declarative base method, in the root `pycds` module.

The declarative base for the tables, `pycds.Base`, automatically receives the schema name returned
`pycds.get_schema_name()`. The schema name can only be
modified by specifying the environment variable `PYCDS_SCHEMA_NAME` in the execution environment of the code.

A table in the database is represented by the class `pycds.<Table>`, where `<Table>` is the name of
the ORM object corresponding to the table in the database. The ORM name and the table name are different but
clearly related. For example, the database table `meta_stations` is represented by the ORM class
`pycds.Station`.
Column names within tables bear a similarly close but not always identical relationship.

To map onto tables already defined in a database, execute

```python
pycds.Base.metadata.create_all(engine)
```

where `engine` is a SQLAlchemy database engine.

### Note: Replaceable objects

Database tables can be mutated "in place": for example, columns can be added, dropped, or renamed.

Other items, including stored procedures and views, are not mutable.
When a change to them is required they must be replaced in their entirety, i.e., dropped and
recreated. Such objects are called replaceable objects, and PyCDS manages them accordingly.

All important changes to a CRMP/PCDS database are now managed by Alembic migrations, including
changes to replaceable objects. (Older versions of PyCDS did not use Alembic, and a different,
now deprecated mechanism was used to make changes.)

### Stored procedures

A stored procedure is a replaceable object (see above), and all such objects are managed
via Alembic migrations. A migration may add, drop, or change a stored procedure.
The stored procedures present in one
migration version of the database can be different than those in another version.

Because of this, it is not
straightforward to get from PyCDS the list of stored procedures that will be present in
any given version of a database. The easiest way to do that is to examine a database at the
migration version of interest.

TODO: Build a tool for this. Existing infrastructure will make this easy.
Alternative: Extract SP definitions to a version-structured sub-module as for views,
and maintain its `__init__.py`.

A stored procedure in the database is is accessed in SQLAlchemy through the standard
`sqlalchemy.func` mechanism in queries.

### Views

A view is a replaceable object (see above), and all such objects are managed via
Alembic migrations. A migration may add, drop, or change a view. The views present in one
migration version of the database can be different than those in another version.

To enable Alembic to work properly, it is necessary retain all
versions of views, not just the latest. The views defined in migration
version `<version>` are stored in the module `pycds.utility_views.version_<version>`
in this directory. The most recent version of each view (frozen in a given release of PyCDS)
is exported by `pycds.utility_views`.

Views are defined using the SQLAlchemy declarative base method, based on an extension to SQLAlchemy
implemented in `pycds/view_helpers.py`.
The declarative base for views, `pycds.utility_views.Base`, automatically receives the schema name returned
`pycds.get_schema_name()`. The schema name can only be
modified by specifying the environment variable `PYCDS_SCHEMA_NAME` in the execution environment of the code.

A view in the database is represented by the class `pycds.utility_views.<View>`, where `<View>` is the name of
the ORM object corresponding to the view in the database. The ORM name and the view name are different but
clearly related. For example, the database view `crmp_network_geoserver` is represented by the ORM class
`pycds.views.CrmpNetworkGeoserver`.
Column names within views bear a similarly close but not always identical relationship.


### Materialized views

A materialized view is a replaceable object (see above), and all such objects are managed via
Alembic migrations. A migration may add, drop, or change a materialized view. The materialized views present in one
migration version of the database can be different than those in another version.

A selection of materialized views is defined in module `pycds.weather_anomaly`. These views support the PCIC
Weather Anomaly application. At present, _all_ predefined materialized views are for the Weather Anomaly application.
Additional materialized views, possibly for other purposes, may be added in future.

To enable Alembic to work properly, it is necessary retain all
versions of materialized views, not just the latest. The materialized views defined in migration
version `<version>` are stored in the module `pycds.weather_anomaly.version_<version>`
in this directory. The most recent version of each view (frozen in a given release of PyCDS)
is exported by `pycds.weather_anomaly`.

Materialized views are defined using the SQLAlchemy declarative base method, based on an extension to SQLAlchemy
in `pycds/materialized_view_helpers.py`. This extension does not at the moment support native materialized views,
which appeared in PostgreSQL 9.3. Instead, materialized views are at present ordinary database tables, and their
contents are maintained by the ORM matview function `refresh()` (see below).
A future release of PyCDS will support native matviews.

The declarative base for materialized views, `pycds.weather_anomaly.Base`, automatically receives the schema name
returned `pycds.get_schema_name()`. The schema name can only be
modified by specifying the environment variable `PYCDS_SCHEMA_NAME` in the execution environment of the code.

A predefined materialized view in the database is represented by the class `pycds.weather_anomaly.<Matview>`,
where `<Matview>` is the name of
the ORM object corresponding to the materialized view in the database.
The ORM name and the materialized view name are different but clearly related.
For example, the database materialized view `daily_max_temperature_mv` is represented by the ORM class
`pycds.weather_anomaly.DailyMaxTemperature`.
Column names within materialized views bear a similarly close but not always identical relationship.

To refresh a materialized view, execute code like the following:

```python
pycds.weather_anomaly.<Matview>.refresh(session)
```

## Database creation and migration

### Introduction

Modifications to the PyCDS schema definition are managed using
[Alembic](https://alembic.sqlalchemy.org/), a database migration management tool based on SQLAlchemy.

In short, Alembic supports and disciplines two processes of database schema change:

- Creation of database migration scripts (Python programs) that modify the schema of a database.

- Application of migrations to specific database instances.

  - In particular, Alembic can be used to *create* a new instance of a ``modelmeta`` database by migrating an
    empty database to the current state. This is described in detail below.

For more information, see the [Alembic tutorial](https://alembic.sqlalchemy.org/en/latest/tutorial.html).

### Specifying the database to operate on

We have customized the Alembic environment manager (`alembic/env.py`) so that it is
possible to operate on any of an arbitrary number of databases defined in `alembic.ini`,
according an `alembic` command line argument. This argument takes the form

```shell script
alembic -x db=<db-label> ...
```

and it must **always** be included in any `alembic` command.

The database to operate upon is specified by `<db-label>` (e.g., `dev`), and the meaning of that name is itself specified
in `alembic.ini`. For any db-name you use, a corresponding entry in `alembic.ini` must exist, of the following
form:

```ini
[<db-label>]
sqlalchemy.url = <DSN>
```

For example:

```ini
[dev]
sqlalchemy.url = postgresql://tester@localhost:30599/pycds_test
```

The file `alembic.ini` already contains several such db-names.
We expect to expand this list as more CRMP-type databases are added to the PCIC stable.

### Specifying the schema within the database

PyCDS enables the user to specify the schema (i.e., named collection of tables, etc.) to be operated upon
using the environment variable `PYCDS_SCHEMA_NAME`.

This has been accomplished in two ways:

- By creating a modified Alembic environment that uses the specified schema name
  (see `env.py#run_migrations_online()` and `env.py#run_migrations_offline()`;
  specifically, `context.configure(version_table_schema=target_metadata.schema)`).
- By using the specified schema name in all migrations. See note below.

**IMPORTANT:** Autogenerated migrations **must** be edited to replace the specific schema name (e.g., `schema='crmp'`)
with the specified schema name (`schema=pycds.get_schema_name()`) wherever it occurs.

All Alembic commands should have the form

```shell script
PYCDS_SCHEMA_NAME=<schema-name> alembic -x db=<db-label> ...
```

### Creating a new database

1. On the server or in the container of your choice:
   1. Create a new database with the desired database name, `<db-name>`
   1. Install extensions PL/Postgres, PL/Python, and PostGIS 2.4
   1. Create an empty schema with the desired name, `<schema-name`
   1. Grant the desired user (name `<user-name>`) write permission to the schema.
      (You may need to created the desired user first.)
   1. For an example of such an arrangement, run the shell script
      `alembic/development/init_test_db/common/create_scripts.sh` and examine
      the SQL scripts it writes to `alembic/development/init_test_db/common`.

1. Add a new DSN for the database, including the appropriate user name,
   to `alembic.ini`. Choose a name by which to refer to it in the Alembic CLI,
   `<db-label>`.

   ```ini
   [<db-label>]
   sqlalchemy.url = postgresql://<user-name>@<server-name>/<db-name>
   ```

1. Initialize the new schema by upgrading it to `head`:

   ```shell script
   PYCDS_SCHEMA_NAME=<schema-name> alembic -x db=<db-label> upgrade head
   ```

### Upgrading an existing PyCDS database (schema)

Once a PyCDS database has been initialized, subsequently created migrations are
simple to apply

To apply all migrations subsequent to the databases's current migration:

```shell script
PYCDS_SCHEMA_NAME=<schema-name> alembic -x db=<db-label> upgrade head
```

To apply migrations up to a specific migration with revision identifier `<rev-id>`:

```shell script
PYCDS_SCHEMA_NAME=<schema-name> alembic -x db=<db-label> upgrade <rev-id>
```

For information on revision identifiers,
see [Partial Revision Identifiers](https://alembic.sqlalchemy.org/en/latest/tutorial.html#partial-revision-identifiers).

### Downgrading an existing PyCDS database (schema)

Migrations can also be undone, by downgrading the database schema to a
migration preceding its current one:

```shell script
PYCDS_SCHEMA_NAME=<schema-name> alembic -x db=<db-label> downgrade <rev-id>
```

For information on revision identifiers,
see [Partial Revision Identifiers](https://alembic.sqlalchemy.org/en/latest/tutorial.html#partial-revision-identifiers).

### Creating a new migration

After the PyCDS ORM has been modified, you will almost certainly want to create an Alembic
migration script to enable existing databases to be upgraded to the new model.

The easiest and likely most reliable way to create a migration script is to autogenerate it using Alembic.
For more details, see the [Alembic documentation](https://alembic.sqlalchemy.org/en/latest/autogenerate.html).

To autogenerate a migration script, you must have a reference database schema for Alembic to compare
to the modified PyCDS ORM.
After Alembic autogenerates the script, you must edit it to ensure completeness, correctness, and
that it respects the specified schema name (see instructions below).

Instructions:

1. Choose or create a reference database schema that is at the latest migration.
   (Or a database schema at an otherwise desired "base" migration, which is unusual but not necessarily wrong.)
   To serve as a reference, a schema need not contain any data.

1. If the reference database is not named in `alembic.ini`, create an entry there for it of the form

   ```ini
   [<db-label>]
   sqlalchemy.url = postgresql://<user-name>@<server-name>/<db-name>
   ```

1. Autogenerate the migration script:

    ```shell script
    PYCDS_SCHEMA_NAME=<schema-name> alembic -x db=<db-label> revision --autogenerate -m "<message>"
    ```

    The `<message>` string should be a succinct description of what the migration accomplishes; for
    example "add name column to Users". This message becomes part of the name and content of the script.

1. Alembic writes a new script to the directory `alembic/versions`. Its name includes
   a unique revision identifier (a SHA) and a version of `<message>`.

1. Review and edit the script to ensure correctness, completeness,
   and that it respects the specified schema name. Specifically:

   1. Review to ensure that it picks up all changes to tables and implements them appropriately.
      In particular, Alembic does not pick up on changes of table or column name, which must be manually converted
      from "drop old name, add new name" to "rename". Some other schema changes are also not detected.
      For more information, see
      [What does Autogenerate Detect (and what does it not detect?)](https://alembic.sqlalchemy.org/en/latest/autogenerate.html#what-does-autogenerate-detect-and-what-does-it-not-detect).

   1. Manually add creation or dropping of the following things not handled by Alembic autogenerate:
      1. Functions
      1. Views
      1. Materialized views

   1. **Ensure that all changes respect the specified schema name** that the user will supply when this
      migration is applied. In particular:
      1. Replace any autogenerated schema name usage (e.g., `schema='crmp'`)
         with the specified schema name (`schema=pycds.get_schema_name()`).
      1. Ensure the specified schema name is used for functions, views and materialized views.
         This should come without special effort due to the structure of how these items are declared,
         but it is worth verifying.

   1. Do this for both upgrade and downgrade functions in the script!

1. Write some tests for the migration. Examples can be found in the existing code.

1. Commit the new migration script and its tests to the repo.

### Creating the initial migration

Since this only needed to be done once and is preserved in the migration
`alembic/versions/522eed334c85_create_initial_database.py`,
this information is largely for archival purposes.

Our modified Alembic environment generally respects the schema name (see above),
but fails in one case: when the specified schema does not (yet) contain an `alembic_version` table.
To remedy this, issue the command

```shell script
PYCDS_SCHEMA_NAME=<schema-name> alembic -x db=<db-label> stamp head
```

which creates the required table.

Following this, issue the command

```shell script
PYCDS_SCHEMA_NAME=<schema-name> alembic -x db=<db-label> revision --autogenerate -m "create initial database"
```

## Testing

### Creating a local test database

It is particularly convenient for testing the Alembic customization (`alembic/env.py`)
and database migrations to have a local test database running.
This is fortunately quite easy, courtesy of Docker.

This repo contains a Dockerfile, `dev.Dockerfile`, which creates a Docker image with PostgreSQL 9.3,
PostGIS 2.4, and PL/Python installed. This configuration matches our test environment,
and approximately matches our production environment, which currently runs PostgreSQL 9.1 and some
compatible version of PostGIS.
(It seems that it is not possible to replicate the production environment environment exactly
in test environments.)

#### Build test database docker image

To build the Docker image locally:

```shell script
docker build -t pycds-test-db -f dev.Dockerfile .
```

The image name `pycds-test-db` is arbitrary, but it is used in the run script.

#### Start test database container and create database, etc.

To start a local Docker container from this image, use the run script:

```shell script
./alembic/development/run-docker-test-db.sh <port> <password>
```

This script starts a container mapped to port `<port>` on `localhost`.

The container creates:
- a PostgreSQL 9.3 server
- with users:
  - user `postgres`, password `<password>`,
  - user `tester`, password `tester`,
- with database `pycds_test`, owned by `tester`,
- with extensions PL/Postgres, PL/Python, and PostGIS 2.4 installed,
- with empty schemas `crmp` and `other`, both owned by `tester`.

To connect to this database on the command line:

```shell script
psql -h localhost -p <port> -U {postgres,tester} -d {crmp,other}
```

The DSN for this database is:

```
postgresql://tester@localhost:<port>/pycds_test
```

#### Stop and remove test database container

To stop the local Docker test database container:

```shell script
./alembic/development/down-docker-test-db.sh
```

This is a pretty trivial convenience, but it's a _convenient_ convenience.

### Unit test data from production

Some data used in the unit tests was sourced from a production database. The steps to produce this were:

1. As database superuser, run CREATE SCHEMA subset AUTHORIZATION <username>;
2. As that user, run `psql -h <db_host> -f create_crmp_subset.sql crmp`.
   This insert a selection of data into the `subset` schema.
3. Then, `pg_dump -h <db_host> -d crmp --schema=subset --data-only --no-owner --no-privileges --no-tablespaces --column-inserts -f pycds/data/crmp_subset_data.sql`
4. Edit this file to remove the `SET search_path...` line,
5. Re-order the data inserts to respect foreign key constraints.
    Default sort is alphabetical and the only changes that should need to be made are ordering `meta_network`,
    `meta_station`, and `meta_history` first and leaving the remaining inserts ordered as is.

### BDD Test Framework `(pytest-describe`)

#### Behaviour Driven Development

Many tests (specifically those for climate baseline helpers and scripts, and for weather anomaly daily and monthly
aggregate views)
are defined with the help of `pytest-describe](https://github.com/ropez/pytest-describe), a pytest plugin
which provides syntactic sugar for the coding necessary to set up Behaviour Driven Development (BDD)-style tests.

Widely adopoted BDD frameworks are [RSpec for Ruby](http://rspec.info/) and [Jasmine for Javascript](https://jasmine.github.io/). pytest-describe implements a simple subset of these frameworks for pytest in Python.

BDD references:

* https://en.wikipedia.org/wiki/Behavior-driven_development
* [Introducing Behaviour Driven Development](https://dannorth.net/introducing-bdd/) - a core document on BDD; clear,
well-written, informative

BDD is a behaviourally focused version of TDD. TDD and BDD are rarely practiced purely, but their principles and
practices even impurely applied can greatly improve unit testing. Specifically, useful BDD practices include:

- Identify a single subject under test (SUT)

- Identify key cases (example behaviours of the SUT), which are often structured hierarchically

    - "hierarchically" means that higher level test conditions persist while lower level ones vary within them

    - for each test condition (deepest level of the hierarchy), one or more assertions is made about the
      behaviour (output) of the SUT

- Each combination of test conditions and test should read like a sentence, e.g.,
  "when A is true, when B is true, when C is true, it (SUT) does an expected thing"

- Code tests for the SUT, structured to set up and tear down test conditions exactly parallel to the identified
  test cases, following the hierarchy of test conditions

- Use a framework that makes it easy to do this, so that the code becomes more nearly self-documenting and the
  output reads easily

    - the latter (easy to read output) is accomplished by running the output of pytest through the script
      scripts/format-pytest-describe; full BDD frameworks provide this kind of reporting out of the box;
      pytest and pytest-describe lack it but it's not hard to add

For example, if the SUT is a function F, with 3 key parameters, A, B, C, one might plan the following tests

```text
for function F
    when A > 0
        when B is null
            when C is an arbitrary string
                result is null
        when B is non-null
            when C is the empty string
                result is 'foo'
            when C is all blanks
                result is 'bar'
            when C is a non-empty, non-blank string
                result is 'baz'
    when A <= 0
        when B is non-null
            when C is an arbitrary string
                result is null
```

This is paralleled exactly by the following test hierarchy using pytest-describe

```python
def describe_F():
    def describe_when_A_is_positive():
        A = 1
        def describe_when_B_is_None():
            B = None
            def describe_when_C_is_any_string():
                C = 'giraffe'
                def it_returns_null():
                    assert F(A,B,C) == None
        def describe_when_B_is_not_None():
            B = [1, 2, 3]
            def describe_when_C_is_empty():
                C = ''
                def it_returns_foo()
                    assert F(A,B,C) == 'foo'
                ...
```

Notes:

- In `pytest-describe`, each test condition is defined by a function whose name begins with `describe_`.

    - In most BDD frameworks, a synonym for `describe` is `context`, which can make the code slightly more
      readable, but it is not defined in pytest-describe.

- In `pytest-describe`, each test proper is defined by a function whose name does **NOT** begin with `describe_`.

    - It need not begin with `test_`, as in pure `pytest`, though it can if desired. It is more readable to begin
      most test function names with `it_`, "it" referring to the subject under test.

- The outermost `describe` names the SUT. It is not required, but it is usual and very helpful.

- The collection of test cases (examples) are not simply the cross product of each possible case of A, B, C;
  often this is unnecessary or unhelpful and in complex systems it can be meaningless or cause errors.

#### Realistic test setup and teardown

In the example above, test condition setup is very simple (variable assignments) and teardown is non-existent.

In more
realistic settings, setup may involve establishing a database and specific database contents, or spinning up some
other substantial subsystem, before test cases can be executed. Equally, teardown can be critical to preserve a
clean environment for the subsequent test conditions. Failure to properly tear down a test environment can give rise
to bugs in the test code that are very difficult to find.

In our usages, test case setup mainly means establishing
specific database contents (using the ORM). Teardown means removing the contents so that the database
is clean for setting up the next test conditions. Because the conditions (and tests) are structured hierarchically,
setup and teardown are focused on one condition at each level of the hierarchy.

#### Fixtures

We use fixtures to set up and tear down database test conditions. Each fixture has the following structure:

- receive database session from parent level
- set up database contents for this level
- yield database session to child level (test or next lower test condition)
- tear down (remove) database contents for this level

This nests setup and teardown correctly through the entire hierarchy, like matching nested
parentheses around tests.

#### Helper function `add_then_delete_objs`

Since the database setup/teardown pattern is ubiquitous, a helper function, `tests.helper.add_then_delete_objs`,
is defined. `add_then_delete_objs` is a generator function that packages up database content setup, session yield,
and content teardown. Because of how generators work, its value must be yielded once to cause setup and a second t
ime to cause teardown. This is most compactly done with a for statement (usually within a fixture):

```python
for sesh in add_then_delete_objs(parent_sesh, [object1, object2, ...]):
    yield sesh
```

For more details see the documentation and code for `add_then_delete_objs`.

In test code, the typical pattern is:

```python
def describe_parent_test_condition():

    @fixture
    def parent_sesh(grandparent_sesh):
        for sesh in add_then_delete_objs(grandparent_sesh, [object1, object2, ...]):
            yield sesh

    def describe_current_test_condition():

        @fixture
        def current_sesh(parent_sesh):
            for sesh in add_then_delete_objs(parent_sesh, [object1, object2, ...]):
                yield sesh


        def describe_child_test_condition():
            ...
```

At each level, the fixture (should) exactly reflect the test condition described by the function name.

All fixtures are available according to the usual lexical scoping for functions. (This is part of what makes
`pytest-describe` useful.)

### Pytest output formatter

The output of `pytest` can be hard to read, particularly if there are many nested levels of test classes (in plain `pytest`) or
of test contexts (as `pytest-describe` encourages us to set up). In plain `pytest` output, each test is listed with its full qualification, which
makes for long lines and much repetition. It would be better if the tests were presented on shorter lines with the
repetition factored out in a hierarchical (multi-level list) view.

Hence `scripts/format-pytest-describe.py`.
It processes the output of `pytest` into a more readable format. Simply pipe the output of `pytest -v` into it.

For quicker review, each listed test is prefixed with a character that indicates the test result:

```text
* `-` : Passed
* `X` : Failed
* `E` : Error
* `o` : Skipped
```

#### Example

Below is the result of running

```
$ py.test -v --tb=short tests | python scripts/format-pytest-describe.py
```

on a somewhat outdated version of the repo (but it gives a good idea of the result):

```text
============================= test session starts ==============================
platform linux2 -- Python 2.7.12, pytest-3.0.5, py-1.4.32, pluggy-0.4.0 -- /home/rglover/code/pycds/py2.7/bin/python2.7
cachedir: .cache
rootdir: /home/rglover/code/pycds, inifile:
plugins: describe-0.11.0
collecting ... collected 87 items


==================== 86 passed, 1 skipped in 64.48 seconds =====================
TESTS:
   tests/test climate baseline helpers.py
      get_or_create_pcic_climate_variables_network
         - test creates the expected new network record (PASSED)
         - test creates no more than one of them (PASSED)
      create_pcic_climate_baseline_variables
         - test returns the expected variables (PASSED)
         - test causes network to be created (PASSED)
         - test creates temperaturise variables[Tx Climatology-maximum-Max.] (PASSED)
         - test creates temperature variables[Tn Climatology-minimum-Min.] (PASSED)
         - test creates precip variable (PASSED)
         - test creates no more than one of each (PASSED)
      load_pcic_climate_baseline_values
         with station and history records
            with an invalid climate variable name
               - test throws an exception (PASSED)
            with a valid climate variable name
               with an invalid network name
                  - test throws an exception (PASSED)
               with a valid network name
                  with a fake source
                     - test loads the values into the database (PASSED)
   tests/test contacts.py
      - test have contacts (PASSED)
      - test contacts relation (PASSED)
   tests/test daily temperature extrema.py
      with 2 networks
         with 1 station per network
            with 1 history hourly per station
               with 1 variable per network
                  with observations for each station variable
                     - it returns one row per unique combo hx var day[DailyMaxTemperature] (PASSED)
                     - it returns one row per unique combo hx var day[DailyMinTemperature] (PASSED)
      with 1 network
         with 1 station
            with 12 hourly history
               with Tmax and Tmin variables
                  with observations for both variables
                     - it returns the expected days and temperature extrema[DailyMaxTemperature-expected0] (PASSED)
                     - it returns the expected days and temperature extrema[DailyMinTemperature-expected1] (PASSED)
            with 1 history daily
               with 1 variable
                  with many observations on different days
                     - it returns the expected number of rows[DailyMaxTemperature] (PASSED)
                     - it returns the expected number of rows[DailyMinTemperature] (PASSED)
                     - it returns the expected days[DailyMaxTemperature] (PASSED)
                     - it returns the expected days[DailyMinTemperature] (PASSED)
                     - it returns the expected coverage[DailyMaxTemperature] (PASSED)
                     - it returns the expected coverage[DailyMinTemperature] (PASSED)
            with 1 history hourly
               with 1 variable
                  with many observations on two different days
                     - it returns two rows[DailyMaxTemperature] (PASSED)
                     - it returns two rows[DailyMinTemperature] (PASSED)
                     - it returns the expected station variables[DailyMaxTemperature] (PASSED)
                     - it returns the expected station variables[DailyMinTemperature] (PASSED)
                     - it returns the expected days[DailyMaxTemperature] (PASSED)
                     - it returns the expected days[DailyMinTemperature] (PASSED)
                     - it returns the expected extreme values[DailyMaxTemperature-statistics0] (PASSED)
                     - it returns the expected extreme values[DailyMinTemperature-statistics1] (PASSED)
                     - it returns the expected data coverages[DailyMaxTemperature] (PASSED)
                     - it returns the expected data coverages[DailyMinTemperature] (PASSED)
                  with many observations in one day bis
                     with pcic flags
                        with pcic flag associations
                           - setup is correct (PASSED)
                           - it excludes all and only discarded observations[DailyMaxTemperature] (PASSED)
                           - it excludes all and only discarded observations[DailyMinTemperature] (PASSED)
                     with native flags
                        with native flag associations
                           - setup is correct (PASSED)
                           - it excludes all and only discarded observations[DailyMaxTemperature] (PASSED)
                           - it excludes all and only discarded observations[DailyMinTemperature] (PASSED)
                  with many observations in one day
                     - it returns a single row[DailyMaxTemperature] (PASSED)
                     - it returns a single row[DailyMinTemperature] (PASSED)
                     - it returns the expected station variable and day[DailyMaxTemperature] (PASSED)
                     - it returns the expected station variable and day[DailyMinTemperature] (PASSED)
                     - it returns the expected extreme value[DailyMaxTemperature-3.0] (PASSED)
                     - it returns the expected extreme value[DailyMinTemperature-1.0] (PASSED)
                     - it returns the expected data coverage[DailyMaxTemperature] (PASSED)
                     - it returns the expected data coverage[DailyMinTemperature] (PASSED)
               with many variables
                  with many observations per variable
                     - it returns exactly the expected variables[DailyMaxTemperature] (PASSED)
                     - it returns exactly the expected variables[DailyMinTemperature] (PASSED)
            with 1 history hourly 1 history daily
               with 1 variable
                  with observations in both histories
                     - it returns one result per history[DailyMaxTemperature] (PASSED)
                     - it returns one result per history[DailyMinTemperature] (PASSED)
                     - it returns the expected coverage[DailyMaxTemperature] (PASSED)
                     - it returns the expected coverage[DailyMinTemperature] (PASSED)
      function effective_day
         - it returns the expected day of observation[max-1-hourly-2000-01-01 07:23] (PASSED)
         - it returns the expected day of observation[max-1-hourly-2000-01-01 16:18] (PASSED)
         - it returns the expected day of observation[max-12-hourly-2000-01-01 07:23] (PASSED)
         - it returns the expected day of observation[max-12-hourly-2000-01-01 16:18] (PASSED)
         - it returns the expected day of observation[min-1-hourly-2000-01-01 07:23] (PASSED)
         - it returns the expected day of observation[min-1-hourly-2000-01-01 16:18] (PASSED)
         - it returns the expected day of observation[min-12-hourly-2000-01-01 07:23] (PASSED)
         - it returns the expected day of observation[min-12-hourly-2000-01-01 16:18] (PASSED)
   tests/test db fixture.py
      - test can create postgis db (PASSED)
      - test can create postgis geometry table model (PASSED)
      - test can create postgis geometry table manual (PASSED)
   tests/test geo.py
      - test can use spatial functions sql (PASSED)
      - test can select spatial functions orm (PASSED)
      - test can select spatial properties (PASSED)
   tests/test ideas.py
      - test basic join (PASSED)
      - test reject discards (PASSED)
      - test aggregate over kind without discards (PASSED)
      - test reject discards 2 (PASSED)
      - test aggregate over kind without discards 2 (PASSED)
   tests/test materialized view helpers.py
      - test viewname (PASSED)
      - test simple view (PASSED)
      - test complex view (PASSED)
      - test counts (PASSED)
   tests/test testdb.py
      - test reflect tables into session (PASSED)
      - test can create test db (PASSED)
      - test can create crmp subset db (PASSED)
   tests/test unique constraints.py
      - test obs raw unique (PASSED)
      - test native flag unique (PASSED)
   tests/test util.py
      o test station table (SKIPPED)
   tests/test view.py
      - test crmp network geoserver (PASSED)
   tests/test view helpers.py
      - test viewname (PASSED)
      - test simple view (PASSED)
      - test complex view (PASSED)
      - test counts (PASSED)
```
