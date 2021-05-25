#! /usr/bin/env python
"""
This script appears to be defunct. The functions `create_test_database`,
`create_test_data` no longer exist, and we presently use different methods
to create test databases. Also, this may now be or soon will be out of date
as migrations modify the database schema.

Leaving this here for archival reasons. (Yeah, I know, that's what git is for.)
"""

import sys
import logging
from argparse import ArgumentParser

from sqlalchemy import create_engine

from pycds.util import create_test_database, create_test_data

if __name__ == "__main__":
    parser = ArgumentParser(
        description="Script to create a test CRMP database and write it to a test database. DSN strings are of form:\n\tdialect+driver://username:password@host:port/database\nExamples:\n\tpostgresql://scott:tiger@localhost/mydatabase\n\tpostgresql+psycopg2://scott:tiger@localhost/mydatabase\n\tpostgresql+pg8000://scott:tiger@localhost/mydatabase"
    )
    parser.add_argument("-t", "--testdsn", help="Destination DSN to write to")
    parser.add_argument(
        "-i",
        "--include-data",
        action="store_true",
        help="Create a small test dataset in the new database",
    )
    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    write_engine = create_engine(args.testdsn)

    create_test_database(write_engine)

    if args.include_data:
        create_test_data(write_engine)
