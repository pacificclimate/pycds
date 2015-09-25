import sys
import string
from setuptools import setup
from setuptools.command.test import test as TestCommand
from pkg_resources import resource_filename
import ctypes
import warnings
__version__ = (0, 0, 21)

class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ['-v', 'tests']
        self.test_suite = True
    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        if not check_for_spatialite():
            raise Exception("Cannot run without libspatialite, which is not installed")
        errno = pytest.main(self.test_args)
        sys.exit(errno)

def check_for_spatialite():
    try:
        ctypes.cdll.LoadLibrary('libspatialite.so')
    except OSError:
        warnings.warn("libspatialite must be installed if you want to run the tests")
        return False

    from sqlalchemy import create_engine

    msg = '''Unfortunately, for using sqlite for testing geographic functionality, you need to manually install pysqlite2 according to the instructions here: http://www.geoalchemy.org/usagenotes.html#notes-for-spatialite
Otherwise, you will not be able to run any of the unit tests for this package for any packages which depend upon it.'''

    try:
        from pysqlite2 import dbapi2
    except ImportError:
        warnings.warn('pysqlite2 package is not installed.\n' + msg)
        return False

    engine = create_engine('sqlite:///{0}'.format('pycds/data/crmp.sqlite'), module=dbapi2, echo=True)
    con = engine.raw_connection().connection
    if not hasattr(con, 'enable_load_extension'):
        warnings.warn('The pysqlite2 package has been built without extension loading support.\n' + msg)
        return False
    else:
        return True

setup(
    name="PyCDS",
    description="An ORM representation of the PCDS and CRMP database",
    keywords="sql database pcds crmp climate meteorology",
    packages=['pycds'],
    version='.'.join(str(d) for d in __version__),
    url="http://www.pacificclimate.org/",
    author="James Hiebert",
    author_email="hiebert@uvic.ca",
    package_data={'pycds': ['data/*.sqlite', 'data/crmp_subset_data.sql']},
    include_package_data=True,
    zip_safe=True,
    scripts = ['scripts/demo.py', 'scripts/mktestdb.py'],
    dependency_links = ['https://github.com/pacificclimate/pysqlite/tarball/master#egg=pysqlite'],
    install_requires = ['SQLAlchemy', 'geoalchemy2', 'psycopg2'],
    tests_require=['pytest', 'pysqlite'],
    cmdclass = {'test': PyTest},

    classifiers='''Development Status :: 3 - Alpha
Environment :: Console
Intended Audience :: Developers
Intended Audience :: Science/Research
License :: OSI Approved :: GNU General Public License v3 (GPLv3)
Operating System :: OS Independent
Programming Language :: Python :: 2.7
Topic :: Internet
Topic :: Scientific/Engineering
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules'''.split('\n')
)
