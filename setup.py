import string
from setuptools import setup

__version__ = (0, 0, 8)

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
