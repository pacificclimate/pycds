# Installation

PyCDS is both a package for ORM access to PCDS/CRMP type databases
and a collection of scripts, primarily Alembic for database management,
plus some (largely disused) scripts for other operations.

## Install as package

PyCDS is published as a package to the 
[PCIC PyPI server](https://pypi.pacificclimate.org/simple),
and any other project that requires it should install it from there.

## Install as an application

Installation is automated through `make`.

To clone the project:

1. `cd` to your parent directory for project code (e.g., `~/code`).
1. `make clone`
1. `cd pycds`

To install the project:

```
$ make install
```
