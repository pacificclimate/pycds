import sys
import string
from setuptools import setup
from setuptools.command.test import test as TestCommand
from pkg_resources import resource_filename
import ctypes
import warnings
__version__ = (0, 0, 13)

class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ['-v']
        self.test_suite = True
    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        global HAVE_SPATIALITE
        if not HAVE_SPATIALITE:
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
    from pysqlite2 import dbapi2
    engine = create_engine('sqlite:///{0}'.format('pycds/data/crmp.sqlite'), module=dbapi2, echo=True)
    con = engine.raw_connection().connection
    if not hasattr(con, 'enable_load_extension'):
        msg = '''The pysqlite2 package has been built without extension loading support.
Unfortunately, for using sqlite for testing geographic functionality, you need to manually install pysqlite2 according to the instructions here: http://www.geoalchemy.org/usagenotes.html#notes-for-spatialite
Otherwise, you will not be able to run any of the unit tests for this package for any packages which depend upon it.
'''
        warnings.warn(msg)
        return False
    else:
        return True

HAVE_SPATIALITE = check_for_spatialite()
    
setup(
    name="PyCDS",
    description="An ORM representation of the PCDS and CRMP database",
    keywords="sql database pcds crmp climate meteorology",
    packages=['pycds'],
    version='.'.join(str(d) for d in __version__),
    url="http://www.pacificclimate.org/",
    author="James Hiebert",
    author_email="hiebert@uvic.ca",
    package_data={'pycds': ['data/*.sqlite']},
    include_package_data=True,
    zip_safe=True,
    scripts = ['scripts/demo.py'],
    install_requires = ['SQLAlchemy==0.8.3dev', 'geoalchemy', 'psycopg2'],
    dependency_links = ['https://bitbucket.org/zzzeek/sqlalchemy/get/rel_0_8.tar.gz#egg=SQLAlchemy-0.8.3dev'],
    tests_require=['pytest', 'pysqlite'],
    cmdclass = {'test': PyTest},

    classifiers='''Development Status :: 2 - Pre-Alpha
Environment :: Console
Intended Audience :: Developers
Intended Audience :: Science/Research
License :: OSI Approved :: GNU General Public License (GPL)
Operating System :: OS Independent
Programming Language :: Python
Topic :: Internet
Topic :: Internet :: WWW/HTTP :: WSGI
Topic :: Scientific/Engineering
Topic :: Software Development :: Libraries :: Python Modules'''.split('\n')
)
