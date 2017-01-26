#! /usr/bin/env python

import sys
import logging
from argparse import ArgumentParser

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
    parser.add_argument("-e", "--exclude", help="Path of file containing native ids of stations to be excluded from loading, one per line")
    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    engine = create_engine(args.dsn)
    session = sessionmaker(bind=engine)()

    f = open(args.file)

    # Header processing: decduced from file and from R code that processes it
    # Headers are optional
    # Header if present, is 2 lines:
    #   - crs?: GEO | ALB | UTM
    #   - some mysterious number, e.g, 21
    # We don't use these header values, and (naturally, therefore) we skip them if present
    line = next(f)
    if line.rstrip(' \0\n') in ['GEO','ALB','UTM']:
        line = next(f)  # header present; skip second header line
    else:
        f.seek(0)  # no header; reset to beginning of file

    # Load excluded station native ids, if provided
    if args.exclude:
        with open(args.exclude) as e:
            exclude = e.readlines()
    else:
        exclude = []
    exclude = [x.strip() for x in exclude]

    load_pcic_climate_baseline_values(session, args.variable, f, exclude)

    session.commit()
