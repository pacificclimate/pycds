# News / Release Notes

## 5.0.0

*Release Date: 2025-Jun-18*

[This release updates base software](https://github.com/pacificclimate/pycds/pull/240)

- Update supported python versions to 3.9-3.13
- Update SQLAlchemy to >2.0
- Migrate pyproject.toml to PEP518 compliance


## 4.5.1

*Release Date: 2024-Jul-31*

This release includes  some improvements to the migration process, and a migration that converts that managed matviews for the Weather Anomaly Viewer into native matviews. 

Changes:
- [Make migration of manual matviews conditional](https://github.com/pacificclimate/pycds/pull/212)
- [Use utility functions when recreating objects to correct permissions](https://github.com/pacificclimate/pycds/pull/214)
- [Remove pcpn_cum_amt from WAV matviews](https://github.com/pacificclimate/pycds/pull/217)

## 4.5.0

*Release Date: 2024-Mar-13*

This release features several adustments (permissions, reordering) to make migrations go more smoothly and some functional enhancements to matviews and tables. Improvements to matviews include adding `start_time` and `end_time` columns to `VarsPerHistory` and adding `vars_id` and `unique_variable_tags` to `CrmpNetworkGeoServer`. The `meta_vars` table has been updated to reject variable names containing newlines.

Changes:
- [Add variable_tags column to CollapsedVariables](https://github.com/pacificclimate/pycds/pull/190)
- [Add check constraint to meta_vars.net_var_name](https://github.com/pacificclimate/pycds/pull/191)
- [Update view CrmpNetworkGoserver with variable tags column](https://github.com/pacificclimate/pycds/pull/194)
- [Update getstationvariable to distinguish climo variables correctly](https://github.com/pacificclimate/pycds/pull/195)
- [Add constraints to reject whitespace on meta_vars](https://github.com/pacificclimate/pycds/pull/197)
- [Reorder revisions after 879f0efa125f](https://github.com/pacificclimate/pycds/pull/203)
- [Grant appropriate permissions on newly created objects in migrations](https://github.com/pacificclimate/pycds/pull/296)
- [Fix permissions omission on one migration](https://github.com/pacificclimate/pycds/pull/207)
- [Add columns for earliest and latest timestamps to vars_per_history_mv](https://github.com/pacificclimate/pycds/pull/210)


## 4.4.0

*Release Date: 2023-Nov-15*

Major change: Add function `variable_tags`.

Changes:
- [Add function variable_tags](https://github.com/pacificclimate/pycds/pull/167)
- [Don't use `*` version specs for dependencies](https://github.com/pacificclimate/pycds/pull/164)
- [Loosen SQLAlchemy-related version requirements](https://github.com/pacificclimate/pycds/pull/163)

## 4.3.0

*Release Date: 2023-Nov-11*

Key change in this release is to install script `manage-views`.

Changes:

- [Document scripts and keepalive usage](https://github.com/pacificclimate/pycds/pull/158)
- [Install selected scripts with Poetry](https://github.com/pacificclimate/pycds/pull/157)

## 4.2.0

*Release Date: 2023-Mar-31*

This release looks big, but quite a bit of it is internal.

The most important change is to fix the script manage-views, which is used
to update the Weather Anomaly matviews. Additionally, there have been some
small tweaks to the ORM and to migrations to fix inconsistencies discovered
late.

Most of the rest is internal: code cleanup, adoption of Poetry as the 
dependency management and build tool, fixes for testing problems.

- [Add crmp2 and update migrations to accommodate crmp database idiosyncracies #144](https://github.com/pacificclimate/pycds/pull/144)
- [Use Poetry #154](https://github.com/pacificclimate/pycds/pull/154)
- [Fix transient failures in CI tests #150](https://github.com/pacificclimate/pycds/pull/150)
- [Convert to pyproject.toml #152](https://github.com/pacificclimate/pycds/pull/152)
- [Fix import and usage of WA views #148](https://github.com/pacificclimate/pycds/pull/148)
- [Change PK in CrmpNetworkGeoserver table declaration #143](https://github.com/pacificclimate/pycds/pull/143)
- [Restore caching and Python matrix #142](https://github.com/pacificclimate/pycds/pull/142)
- [Add not-null constraints to FKs in obs_raw_native_flags and obs_raw_pcic_flags #141](https://github.com/pacificclimate/pycds/pull/141)
- [Upgrade pytest #140](https://github.com/pacificclimate/pycds/pull/140)
- [Replace plpythonu with plpython3u #139](https://github.com/pacificclimate/pycds/pull/139)
- [Code cleanup #138](https://github.com/pacificclimate/pycds/pull/138)

## 4.0.1

*Release Date: 2022-Dec-07*

Bugfixes for migrations, plus a couple of minor things.

- [Fix NEWS.md](https://github.com/pacificclimate/pycds/pull/131)
- [Add server default for meta_station.publish in migration](https://github.com/pacificclimate/pycds/pull/130)
- [Add metnorth2 databases to alembic.ini](https://github.com/pacificclimate/pycds/pull/128)
- [Use su role to create/drop extension citext](https://github.com/pacificclimate/pycds/pull/127)


## 4.0.0

*Release Date: 2022-Feb-03*

- [Remove PK constraint from station_obs_stats_mv](https://github.com/pacificclimate/pycds/pull/121)
- [Add publish flag to meta_stations](https://github.com/pacificclimate/pycds/pull/119)
- [Add sqlalchemy-citext to install dependencies in setup.py](https://github.com/pacificclimate/pycds/pull/117)
- [Change StationObservationStats PK to history_id](https://github.com/pacificclimate/pycds/pull/114)
- [Refactor tests](https://github.com/pacificclimate/pycds/pull/112)
- [Add NOT NULL constraints to certain Variable (meta_vars) columns](https://github.com/pacificclimate/pycds/pull/111)
- [Update test infrastructure](https://github.com/pacificclimate/pycds/pull/109)
- [Change column meta_vars.net_var_name type to citext](https://github.com/pacificclimate/pycds/pull/)

## 3.3.0

*Release Date: 2022-Feb-03*

Note: An earlier version tagged 3.3.0 was released. That release was 
withdrawn and re-released as the present version.

This is a large and belated release. It includes several different classes of
change, including:

- Migrations
- Bug fixes to ORM and migrations
- Refactoring of Alembic content to make it possible to migrate a (test)
  test database in a client app to the desired revision.
- Substantial refactoring of unit tests.

This release contains no breaking changes.

- [Pin zipp dependency to "==3.5.0"](https://github.com/pacificclimate/pycds/pull/104)
- [Replace pip with pipenv](https://github.com/pacificclimate/pycds/pull/103)
- [Fix copy-conflict warnings](https://github.com/pacificclimate/pycds/pull/101)
- [Drop table meta_climo_attrs](https://github.com/pacificclimate/pycds/pull/100)
- [Apply Black](https://github.com/pacificclimate/pycds/pull/97)
- [Cleanup](https://github.com/pacificclimate/pycds/pull/96)
- [Add missing indexes](https://github.com/pacificclimate/pycds/pull/93)
- [Convert unique constraints to primary key declarations](https://github.com/pacificclimate/pycds/pull/92)
- [Add obs_raw indexes](https://github.com/pacificclimate/pycds/pull/89)
- [Refactor replaceable objects](https://github.com/pacificclimate/pycds/pull/85)
- [Add SET/RESET ROLE to migrations](https://github.com/pacificclimate/pycds/pull/82)
- [Add conditional migration for native matview VarsPerHistory](https://github.com/pacificclimate/pycds/pull/73)

## 3.2.4

*Release Date: 2022-Jan-28*
WITHDRAWN

## 3.2.3

(not a release)
WITHDRAWN

## 3.2.2

*Release Date: 2022-Jan-27*
WITHDRAWN

## 3.2.1

*Release Date: 2022-Jan-25*
WITHDRAWN

## 3.2.0

*Release Date: 2021-Jan-11*

- [Include all pycds (sub)packages](https://github.com/pacificclimate/pycds/pull/71)
- [Repackage so that Alembic migrations can be performed from client apps](https://github.com/pacificclimate/pycds/pull/70)

## 3.1.1

*Release Date: 2021-Jan-11*

- Publish all subpackages (using `find_packages`).

## 3.1.0

*Release Date: 2021-Jan-08*

- [Fix weather anomaly matviews refresh](https://github.com/pacificclimate/pycds/pull/65)
- [Establish GitHub Actions for CI, Docker publishing, and PyPI publishing](https://github.com/pacificclimate/pycds/pull/62)

## 3.0.0

*Release Date: 2020-Feb-13*

This is a major new release. Highlights:

* Python 3.6+ compatible only. Releases >=3.0.0 will no longer support Py 2.7. Little or no maintenance will be done
on versions <3.0.0 (which remains Py 2.7 compatible).
* Enable use of variable schema name, specified by environment variable `PYCDS_SCHEMA_NAME`.
* Establish database migration control using Alembic.
* Add migrations that set up standard PyCDS tables, functions, views, and materialized views.
* Pin dependency versions.

Major / dominant changes:

* [Add migration version-check function](https://github.com/pacificclimate/pycds/pull/58)
* [Add migration for weather anomaly materialized views](https://github.com/pacificclimate/pycds/pull/55)
* [Add migration for utility views](https://github.com/pacificclimate/pycds/pull/53)
* [Add migration for functions (stored procedures)](https://github.com/pacificclimate/pycds/pull/52)
* [Initialize migration management](https://github.com/pacificclimate/pycds/pull/50)
* [Make PyCDS schema name agnostic](https://github.com/pacificclimate/pycds/pull/44)
* [Rationalize test fixtures](https://github.com/pacificclimate/pycds/pull/42)
* [Drop support for Python <3.6](https://github.com/pacificclimate/pycds/pull/40)
* [Modify PyCDS table definitions to bring it to canonical current state of CRMP](https://github.com/pacificclimate/pycds/pull/35)

Minor changes:

* [Add index definitions to the obs_raw table](https://github.com/pacificclimate/pycds/pull/24)
* [Prod fixes for load and verify baseline](https://github.com/pacificclimate/pycds/pull/18)

# 2.2.1

*Release Date: 2017-Oct-24*

* Fixes a query in the util module where both columns were ambiguously named "obs_time"

# 2.2.0

*Release Date: 2017-Oct-17*

* Use regular tables for materialized views
* Use schema 'crmp' consistently throughout code
* Fix bugs discovered in script ``verify-load-climate-baseline-values`` while processing baseline data files:
  * Handle exceptions raised during verification
* Fix bugs discovered in script ``load-climate-baseline-values`` while processing baseline data files:
  * Strip spaces from datum for comparison to -9999 (missing value)
  * Fix counts of climatology stations
  * Filter climatology variables by network (PCIC Derived Variables network)
  * Improve logging
