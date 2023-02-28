# News / Release Notes

## 3.3.1

*Release Date: 2023-Feb-28*

(Side release to fix a bug in an old release still in use.)

- [Fix script manage-views](https://github.com/pacificclimate/pycds/issues/146)

## 3.3.0

*Release Date: 2022-Feb-03*

Note: 3.3.0 was originally released Jan 31, then withdrawn and re-released with
only non-breaking content on Feb 3.

Note: Several previous "patch" releases (which were
nothing of the kind) have been permanently withdrawn (specifically
3.2.1, ..., 3.2.4).

This release does not include breaking changes.
It is, however, a large and belated release. It includes several different
classes of change, including:

- Migrations
- Bug fixes to ORM and migrations
- Refactoring of Alembic content to make it possible to migrate a (test)
  test database in a client app to the desired revision.

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
