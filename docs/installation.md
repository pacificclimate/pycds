# Installation

PyCDS is both a package for ORM access to PCDS/CRMP type databases
and a collection of scripts, primarily Alembic for database management,
plus some (largely disused) scripts for other operations.

## Install as package or as an application

PyCDS is published as a package to the 
[PCIC PyPI server](https://pypi.pacificclimate.org/simple),
and any other project that requires it should install it as follows:

```text
pip install -i https://pypi.pacificclimate.org/simple pycds
```

Note: Alembic cannot be run from a pure package installation. To perform
Alembic operations (e.g., migrate a database), install the project for
development as described below.

## Install for development

### Clone the project

1. `cd` to your parent directory for project code (e.g., `~/code`).
1. `git clone git@github.com:pacificclimate/pycds.git`
1. `cd pycds`

### Install Poetry package manager

We use [Poetry](https://python-poetry.org/) to manage package dependencies 
and installation.
To install Poetry, we recommend using the 
[official installer](https://python-poetry.org/docs/#installation):

```text
curl -sSL https://install.python-poetry.org | python3 -
```

### Install the project using Poetry

You can install the project in any version of Python >= 3.7. By default,
Poetry uses the base version of Python 3 installed on your system, and without
further intervention the project will be installed in a virtual environment
based on that.

```text
poetry install
```

If you have other versions of Python installed on your system then you can
add virtual environments based on them using the 
[environment management](https://python-poetry.org/docs/managing-environments/)
commands. (You do not need to use Pyenv to install the other Pythons; any
method will work. You can skip the Pyenv discussion.)

Once you have activated an environment, you can issue `poetry install` again
to install it in that environment. Environments are persistent, so one
installation is sufficient unless you are actually changing the dependencies
or other aspects of the installation. You may switch at will between different
environments.
