# News / Release Notes

## 3.2.1

*Release Date: 2022-Jan-25*

- Minor update to README
- [Fix StationObservationStats primary key](https://github.com/pacificclimate/pycds/pull/114)

## 3.2.0

*Release Date: 2021-Jan-11*

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
