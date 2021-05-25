#! /usr/bin/env python

import sys
import logging
from argparse import ArgumentParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import pycds.weather_anomaly
import pycds.climate_baseline_helpers
from pycds.climate_baseline_helpers import load_pcic_climate_baseline_values

if __name__ == "__main__":
    parser = ArgumentParser(
        description="""Script to load a file of climate baseline values for a single variable into a database.
DSN strings are of form:
    dialect+driver://username:password@host:port/database
Examples:
    postgresql://scott:tiger@localhost/mydatabase
    postgresql+psycopg2://scott:tiger@localhost/mydatabase
    postgresql+pg8000://scott:tiger@localhost/mydatabase
"""
    )
    parser.add_argument(
        "-d", "--dsn", help="Database DSN in which to create new network"
    )
    parser.add_argument(
        "-v", "--variable", help="Name of variable to be loaded"
    )
    parser.add_argument(
        "-f",
        "--file",
        help="Path of file containing climate baseline values to be loaded",
    )
    parser.add_argument(
        "-x",
        "--exclude",
        help="Path of file containing native ids of stations to be excluded from loading, one per line",
    )
    log_level_choices = "NOTSET DEBUG INFO WARNING ERROR CRITICAL".split()
    parser.add_argument(
        "-s",
        "--scriptloglevel",
        help="Script logging level",
        choices=log_level_choices,
        default="INFO",
    )
    parser.add_argument(
        "-e",
        "--dbengloglevel",
        help="Database engine logging level",
        choices=log_level_choices,
        default="WARNING",
    )
    args = parser.parse_args()

    script_logger = logging.getLogger(__name__)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s: %(message)s"
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    sa_logger = logging.getLogger("sqlalchemy.engine")
    sa_logger.addHandler(handler)
    sa_logger.setLevel(getattr(logging, args.dbengloglevel))

    script_logger = logging.getLogger(pycds.climate_baseline_helpers.__name__)
    script_logger.addHandler(handler)
    script_logger.setLevel(getattr(logging, args.scriptloglevel))

    engine = create_engine(args.dsn)
    session = sessionmaker(bind=engine)()

    script_logger.debug("creating all ORM objects")
    pycds.Base.metadata.create_all(bind=engine)
    pycds.weather_anomaly.Base.metadata.create_all(bind=engine)

    def search_path():
        sp = session.execute("SHOW search_path")
        return ",".join([r.search_path for r in sp])

    script_logger.debug("search_path before: {}".format(search_path()))
    session.execute("SET search_path TO crmp, public")
    script_logger.debug("search_path after: {}".format(search_path()))

    f = open(args.file)

    # Header processing: decduced from file and from R code that processes it
    # Headers are optional
    # Header if present, is 2 lines:
    #   - crs?: GEO | ALB | UTM
    #   - some mysterious number, e.g, 21
    # We don't use these header values, and (naturally, therefore) we skip them if present
    line = next(f)
    if line.rstrip(" \0\n") in ["GEO", "ALB", "UTM"]:
        script_logger.debug(
            "Header lines detected; skipping first 2 lines in file"
        )
        line = next(f)  # header present; skip second header line
    else:
        script_logger.debug("No header lines detected")
        f.seek(0)  # no header; reset to beginning of file

    # Load excluded station native ids, if provided
    if args.exclude:
        with open(args.exclude) as e:
            exclude = e.readlines()
    else:
        exclude = []
    exclude = [x.strip() for x in exclude]

    try:
        load_pcic_climate_baseline_values(
            session, args.variable, f, exclude=exclude
        )
        session.commit()
    finally:
        session.close()
