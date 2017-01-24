import sys
import logging
from argparse import ArgumentParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pycds.weather_anomaly import \
    DailyMaxTemperature, DailyMinTemperature, \
    MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, \
    MonthlyTotalPrecipitation

if __name__ == '__main__':
    parser = ArgumentParser(description="""Script to refresh materialized views in the PCDS/CRMP database.
DSN strings are of form:
    dialect+driver://username:password@host:port/database
Examples:
    postgresql://scott:tiger@localhost/mydatabase
    postgresql+psycopg2://scott:tiger@localhost/mydatabase
    postgresql+pg8000://scott:tiger@localhost/mydatabase
""")
    parser.add_argument("-d", "--dsn", help="Database DSN in which to create new network")
    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    engine = create_engine(args.dsn)
    session = sessionmaker(bind=engine)()

    views = [
        DailyMaxTemperature, DailyMinTemperature,
        MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature,
        MonthlyTotalPrecipitation
    ]
    for view in views:
        logging.info("Refreshing '{}'".format(view.viewname()))
        view.refresh(session)