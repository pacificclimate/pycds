import sys
from setuptools import setup
from setuptools.command.test import test as TestCommand

__version__ = (2, 2, 1)


class PyTest(TestCommand):

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ['-v', 'tests']
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
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
    package_data={'pycds': ['data/crmp_subset_data.sql']},
    include_package_data=True,
    zip_safe=True,
    scripts=[
        'scripts/demo.py',
        'scripts/mktestdb.py',
        'scripts/format-pytest.py',
        'scripts/load-climate-baseline-values.py',
        'scripts/verify-load-climate-baseline-values.py',
        'scripts/manage-views.py'
    ],
    install_requires=['SQLAlchemy', 'geoalchemy2', 'psycopg2'],
    tests_require=['pytest'],
    cmdclass={'test': PyTest},

    classifiers='''Development Status :: 5 - Production/Stable
Environment :: Console
Intended Audience :: Developers
Intended Audience :: Science/Research
License :: OSI Approved :: GNU General Public License v3 (GPLv3)
Operating System :: OS Independent
Programming Language :: Python :: 2.7
Programming Language :: Python :: 3.4
Programming Language :: Python :: 3.5
Topic :: Internet
Topic :: Scientific/Engineering
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules'''.split('\n')
)
