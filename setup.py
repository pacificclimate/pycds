import sys
import string
from setuptools import setup
from setuptools.command.test import test as TestCommand
import ctypes

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

try:
    ctypes.cdll.LoadLibrary('libspatialite.so')
    HAVE_SPATIALITE = True
except OSError:
    import warnings
    warnings.warn("libspatialite must be installed if you want to run the tests")
    HAVE_SPATIALITE = False
        
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
