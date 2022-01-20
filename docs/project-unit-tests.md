# Project unit tests

## Table of contents

- [Continuous Integration testing](#continuous-integration-testing)
- [Docker container for running tests](#docker-container-for-running-tests)
    - [Introduction](#introduction)
    - [Instructions](#instructions)
    - [Notes and caveats](#notes-and-caveats)
- [BDD Test Framework (`pytest-describe`)](#bdd-test-framework-pytest-describe)
    - [Behaviour Driven Development](#behaviour-driven-development)
    - [Realistic test setup and teardown](#realistic-test-setup-and-teardown)
    - [Fixtures](#fixtures)
    - [Helper function `add_then_delete_objs`](#helper-function-add_then_delete_objs)
- [Pytest output formatter](#pytest-output-formatter)

## Unit test organization

TODO: Document other types of tests, e.g., alembic tooling and extensions.

### Unit tests for migrations

Migration tests, aka smoke tests, test the operation of a single migration and whether it modifies the schema (structure) as intended.

Migration tests are organized in a way that parallels the arrangement of the migration scripts they test. Per-migration tests are placed in subdirectories of `tests/alembic_migrations/versions/`. Subdirectories are named very similarly to as the migrations they test. The prefix `v_` (for version/revision) is used so that if necessary the directories can be treated as Python modules. Each such directory contains tests for the results of that migration alone.

### Behavioural tests

Behavioural tests test the _behaviour_ of a database schema object after migration. For example, test whether a view or materialized view contains the rows expected given a certain database content.

Behavioural tests are placed in the directory `tests/behavioural`. 

## Continuous Integration testing

Project unit tests are run automatically in an environment approximating our
production environment. This is done by the GitHub workflow `python-ci`.

## Docker container for running tests

### Introduction

It is difficult to establish an appropriate run-time environment on
a workstation for running tests (particularly since the transition to 
Ubuntu 20.04).

To fill that gap, we've defined Docker infrastructure that allows you to 
build and run a Docker container for testing that is equivalent to the 
production environment. The infrastructure is in `docker/local-pytest/`.

### Instructions

1. **Advance prep**

    Do each of the following things **once per workstation**.
    
    1. Configure Docker user namespace mapping.
    
        1. Clone [`pdp-docker`](https://github.com/pacificclimate/pdp-docker).
     
        1. Follow the instructions in the `pdp-docker` documentation:
         [Setting up Docker namespace remapping (with recommended parameters)](https://github.com/pacificclimate/pdp-docker#setting-up-docker-namespace-remapping-with-recommended-parameters).
               
1. **Build the image**

    The image need only be (re)built when:
    
    1. the project is first cloned, or
    1. the local-test Dockerfile changes.
    
    To build the image:
    
    ```
    make local-pytest-image
    ```
   
1. **Mount the gluster `/storage` volume**
   
    Mount locally to `/storage` so that those data files are accessible on 
    your workstation.

    ```
    sudo mount -t cifs -o username=XXXX@uvic.ca //pcic-storage.pcic.uvic.ca/storage/ /storage
    ```

1. **Start the test container**

    ```
    make local-pytest-run 
    ```
    
    This starts the container, installs the local codebase, gives you a 
    bash shell. You should see a standard bash prompt.

1. **Change code and run tests**

   Each time you wish to run tests on your local codebase, enter a suitable
   command at the prompt. For example:
    
   ```
   pipenv run pytest -v -m "not slow" --tb=short tests -x
   ```
   
   Alternatively, run                               

   ```
   pipenv shell
   ```
   
   to obtain a shell in which the virtual environment created by `pipenv` is 
   activated. All commands run from this shell take place within the virtual 
   environment. To exit this shell (but not the container), enter `exit`.

   
1. **Do not stop the container until you have finished all changes and
   testing you wish to make for a given session.** 
   It is far more time efficient run tests inside the same container 
   (avoiding startup time) than to restart the container for each test.
    
   Your local codebase is mounted to the container. Any code changes you make 
   externally (in your local filesystem) are reflected "live" inside the 
   container.

2. **Stop the test container**

    When you have completed a develop-and-test session and no longer wish to
    have the test container running, enter Ctrl+D or `exit` on the 
    command line. The container is stopped and automatically removed.

### Notes and caveats

1. As noted above, running tests in the test container in read/write mode 
leaves problematic pycache junk behind in the host filesystem. This can be 
cleaned up by running `py3clean .`.

## BDD Test Framework (`pytest-describe`)

Some project unit tests are written in a BDD (Behaviour Driven Development) 
style.

Since this approach is so uncommon in the Python world, I have since come
to repent of using it, despite its strengths. However, until some angel
refactors these tests into the more familiar form, this is what we have.

### Behaviour Driven Development

Many tests (specifically those for climate baseline helpers and scripts, and for weather anomaly daily and monthly
aggregate views)
are defined with the help of [`pytest-describe`](https://github.com/ropez/pytest-describe), a pytest plugin
which provides syntactic sugar for the coding necessary to set up Behaviour Driven Development (BDD)-style tests.

Widely adopoted BDD frameworks are [RSpec for Ruby](http://rspec.info/) and 
[Jasmine for JavaScript](https://jasmine.github.io/). 
`pytest-describe` implements a subset of these frameworks for pytest in 
Python.

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
  "when A is true, and B is true, and C is true, the SUT does the following
   expected thing(s)", where A, B, and C are test conditions established
   (typically) hierarchically.

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

### Realistic test setup and teardown

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

### Fixtures

We use fixtures to set up and tear down database test conditions. Each fixture has the following structure:

- receive database session from parent level
- set up database contents for this level
- yield database session to child level (test or next lower test condition)
- tear down (remove) database contents for this level

This nests setup and teardown correctly through the entire hierarchy, like matching nested
parentheses around tests.

### Helper function `add_then_delete_objs`

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

## Pytest output formatter

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

#### Example output

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

