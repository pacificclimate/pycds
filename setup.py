import string
from setuptools import setup

__version__ = (0, 0, 2)

setup(
    name="PyCDS",
    description="An ORM representation of the PCDS and CRMP database",
    keywords="sql database pcds crmp climate meteorology",
    packages=['pycds'],
    version='.'.join(str(d) for d in __version__),
    url="http://www.pacificclimate.org/",
    author="James Hiebert",
    author_email="hiebert@uvic.ca",
#    namespace_packages=['pydap', 'pydap.handlers'],
#    entry_points='''
#                 ''',
#    install_requires=['pydap.handlers.sql'],
    zip_safe=True,
    scripts = ['scripts/demo.py'],
    install_requires = ['sqlalchemy', 'psycopg2'],
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
