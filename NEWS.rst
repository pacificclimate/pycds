News / Release Notes
====================

2.2.0
-----

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
