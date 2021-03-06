#! /usr/bin/env python

import sys
import logging
from argparse import ArgumentParser

from sqlalchemy import create_engine

from pycds.util import create_test_database, create_test_data

if __name__ == '__main__':
    parser = ArgumentParser(description="Script to create a test CRMP database and write it to a test database. DSN strings are of form:\n\tdialect+driver://username:password@host:port/database\nExamples:\n\tpostgresql://scott:tiger@localhost/mydatabase\n\tpostgresql+psycopg2://scott:tiger@localhost/mydatabase\n\tpostgresql+pg8000://scott:tiger@localhost/mydatabase")
    parser.add_argument("-t",
                        "--testdsn",
                        help="Destination DSN to write to")
    parser.add_argument("-i",
                        "--include-data",
                        action='store_true',
                        help="Create a small test dataset in the new database")
    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    write_engine = create_engine(args.testdsn)

    create_test_database(write_engine)

    if(args.include_data):
        create_test_data(write_engine)
