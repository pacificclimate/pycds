import sys
import string
from setuptools import setup
from setuptools.command.test import test as TestCommand

__version__ = (0, 0, 12)

class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True
    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)
                                                                        
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
    install_requires = ['SQLAlchemy==0.8.3dev', 'psycopg2'],
    dependency_links = ['https://bitbucket.org/zzzeek/sqlalchemy/get/rel_0_8.tar.gz#egg=SQLAlchemy-0.8.3dev'],
    test_requires=['pytest'],
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
