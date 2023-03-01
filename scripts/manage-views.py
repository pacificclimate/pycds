#! /usr/bin/env python

import sys
import logging
from argparse import ArgumentParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import pycds.manage_views
from pycds.manage_views import manage_views
import pycds.orm.view_base

if __name__ == "__main__":
    parser = ArgumentParser(
        description="""Script to manage views in the PCDS/CRMP database.

DSN strings are of form:
    dialect+driver://username:password@host:port/database
Examples:
    postgresql://scott:tiger@localhost/mydatabase
    postgresql+psycopg2://scott:tiger@localhost/mydatabase
    postgresql+pg8000://scott:tiger@localhost/mydatabase
"""
    )
    parser.add_argument("-d", "--dsn", help="Database DSN in which to manage views")
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
    parser.add_argument("operation", help="Operation to perform", choices=["refresh"])
    parser.add_argument(
        "views", help="Views to affect", choices=["daily", "monthly-only", "all"],
    )
    args = parser.parse_args()

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s: %(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    sa_logger = logging.getLogger("sqlalchemy.engine")
    sa_logger.addHandler(handler)
    sa_logger.setLevel(getattr(logging, args.dbengloglevel))

    mv_logger = logging.getLogger(pycds.manage_views.__name__)
    mv_logger.addHandler(handler)
    mv_logger.setLevel(getattr(logging, args.scriptloglevel))

    mv_logger.debug("Creating engine")
    engine = create_engine(
        args.dsn,
        # This might not be necessary, but it can't hurt.
        pool_pre_ping=True,
        # The following keepalive args fix a connection timeout problem that interrupted
        # the refreshes. Note: These args are specific to psycopg2. They may not work
        # with other DBAPIs.
        connect_args={
            "keepalives": 1,
            "keepalives_idle": 60,
            "keepalives_interval": 10,
            "keepalives_count": 5,
        },
    )
    session = sessionmaker(bind=engine)()

    mv_logger.debug("Creating all ORM objects")
    pycds.Base.metadata.create_all(bind=engine)
    pycds.orm.view_base.Base.metadata.create_all(bind=engine)

    mv_logger.debug(f"Executing '{args.operation}' on views {args.views}")
    manage_views(session, args.operation, args.views)

    session.commit()

    mv_logger.info("Done")
