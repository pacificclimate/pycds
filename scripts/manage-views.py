#! /usr/bin/env python

import sys
import logging
from argparse import ArgumentParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import pycds.manage_views
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
    parser.add_argument('operation', help="Operation to perform",
                        choices=['create', 'refresh'])
    parser.add_argument('views', help="Views to affect",
                        choices=['daily', 'monthly-only', 'all'])
    args = parser.parse_args()

    logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)
    logger = logging.getLogger(pycds.manage_views.__name__)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    engine = create_engine(args.dsn)
    session = sessionmaker(bind=engine)()

    logger.debug('creating all ORM objects')
    pycds.Base.metadata.create_all(bind=engine)
    pycds.weather_anomaly.Base.metadata.create_all(bind=engine)

    def search_path():
        sp = session.execute('SHOW search_path')
        return ','.join([r.search_path for r in sp])

    logger.debug('search_path before: {}'.format(search_path()))
    session.execute('SET search_path TO crmp, public')
    logger.debug('search_path after: {}'.format(search_path()))

    manage_views(session, args.operation, args.views)

    session.commit()
