=====
PyCDS
=====

The PyCDS package is a Python package that provides an `Object Relational Mapping (ORM) <http://en.wikipedia.org/wiki/Object-relational_mapping>`_ layer for accessing British Columbia (BC) meteorological observations from a relational database. BC's long-term weather archive, the Provincial Climate Data Set (PCDS), is a collaboration between the Climate Related Monitoring Project (CRMP) and the `Pacific Climate Impacts Consortium (PCIC) <http://www.pacificclimate.org/>`_. With this package, one can recreate the database schema in `PostgreSQL <http://www.postgresql.org>`_ or `SQLite <http://www.sqlite.org>`_ and/or use the package as an object mapper for programmatic database access. PyCDS uses `SQLAlchemy <http://www.sqlalchemy.org>`_ to provide the ORM layer.

--------------
How to Install
--------------

One can install PyCDS using the standard methods of any other Python package.

1. clone our repository and run the setup script

    $ git clone https://github.com/pacificclimate/pycds
    $ cd pycds
    $ python setup.py install

2. or just point `pip` to our `GitHub repo <https://github.com/pacificclimate/pycds>`_:

    $ pip install git+https://github.com/pacificclimate/pycds

----------
Background
----------

Provincial Climate Data Set
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Provincial Climate Data Set (PCDS) is an archive of meteorological observations for BC collected by federal agencies, BC ministries and crown corporations and dating back to the late 1800's. The archive consists of a relational database that models the data collected by multiple agencies (a.k.a. "networks") at multiple locations (a.k.a. "stations").

Climate Related Monitoring Program
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Climate Related Monitoring Program (CRMP) is a collaborative effort between several BC ministries, crown corporations, and PCIC. Its purpose is to opportunistically leverage weather observations that are being collected for operational uses and utilize them for long-term climate monitoring. More information on the program can be found `here <http://www.env.gov.bc.ca/epd/wamr/crmp.htm>`_.

