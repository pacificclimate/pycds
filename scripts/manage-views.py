#! /usr/bin/env python

import sys
import logging
from argparse import ArgumentParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pycds.materialized_view_helpers import MaterializedViewMixin
from pycds.weather_anomaly import \
    DailyMaxTemperature, DailyMinTemperature, \
    MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, \
    MonthlyTotalPrecipitation

daily_views = [DailyMaxTemperature, DailyMinTemperature]
monthly_views = [MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, MonthlyTotalPrecipitation]

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
    parser.add_argument('views', help="Views to refresh (daily | monthly | all)",
                        choices=['daily', 'monthly', 'all'])
    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    engine = create_engine(args.dsn)
    session = sessionmaker(bind=engine)()

    views = {
        'daily': daily_views,
        'monthly': monthly_views,
        'all': daily_views + monthly_views  # order matters
    }[args.views]

    for view in views:
        logging.info("{} '{}'".format(args.operation.capitalize(), view.viewname()))
        getattr(view, args.operation)()
