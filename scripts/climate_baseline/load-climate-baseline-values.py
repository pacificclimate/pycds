import sys
import logging
from argparse import ArgumentParser
from itertools import islice

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pycds.climate_baseline_helpers import load_pcic_climate_baseline_values

if __name__ == '__main__':
    parser = ArgumentParser(description="""Script to load a file of climate baseline values for a single variable into a database.
DSN strings are of form:
    dialect+driver://username:password@host:port/database
Examples:
    postgresql://scott:tiger@localhost/mydatabase
    postgresql+psycopg2://scott:tiger@localhost/mydatabase
    postgresql+pg8000://scott:tiger@localhost/mydatabase
""")
    parser.add_argument("-d", "--dsn", help="Database DSN in which to create new network")
    parser.add_argument("-v", "--variable", help="Name of variable to be loaded")
    parser.add_argument("-f", "--file", help="Path of file containing climate baseline values to be loaded")
    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    engine = create_engine(args.dsn)
    session = sessionmaker(bind=engine)()

    f = open(args.file)

    line = next(f)
    if line.rstrip(' \0\n') in ['GEO','ALB','UTM']:
        line = next(f)  # header present; skip second header line
    else:
        f.seek(0)  # no header; reset to beginning of file

    load_pcic_climate_baseline_values(session, args.variable, f)
