#! /usr/bin/env python

import sys
import logging
from argparse import ArgumentParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import pycds
from pycds.manage_views import manage_views
import pycds.weather_anomaly

if __name__ == '__main__':
    parser = ArgumentParser(description="""Script to manage views in the PCDS/CRMP database.

DSN strings are of form:
    dialect+driver://username:password@host:port/database
Examples:
    postgresql://scott:tiger@localhost/mydatabase
    postgresql+psycopg2://scott:tiger@localhost/mydatabase
    postgresql+pg8000://scott:tiger@localhost/mydatabase
""")
    parser.add_argument('-d', '--dsn', help='Database DSN in which to manage views')
    parser.add_argument('operation', help="Operation to perform (create | refresh)",
                        choices=['create', 'refresh'])
    parser.add_argument('views', help="Views to affect",
                        choices=['base', 'daily', 'monthly', 'all', 'base-only', 'daily-only', 'monthly-only'])
    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    engine = create_engine(args.dsn)
    session = sessionmaker(bind=engine)()

    pycds.Base.metadata.create_all(bind=engine)
    pycds.weather_anomaly.Base.metadata.create_all(bind=engine)

    session.execute('SET search_path TO crmp, public')

    manage_views(session, args.operation, args.views)

    session.commit()
