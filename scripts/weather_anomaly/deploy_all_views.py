import sys
import logging
from argparse import ArgumentParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pycds.materialized_view_helpers import MaterializedViewMixin

from views import all_views

if __name__ == '__main__':
    parser = ArgumentParser(description="""
Script to deploy weather anomaly views (materialized and regular) to a specified database.
All views are first dropped, then (re)created, then optionally refreshed.
DSN strings are of the form:
    dialect+driver://username:password@host:port/database
Examples:
    postgresql://scott:tiger@localhost/mydatabase
    postgresql+psycopg2://scott:tiger@localhost/mydatabase
    postgresql+pg8000://scott:tiger@localhost/mydatabase""")
    parser.add_argument('-d', '--dsn', help='Database DSN to which to deploy views')
    parser.add_argument('-r', '--refresh', action='store_true', help='Refresh materialized views after deployment')
    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    engine = create_engine(args.dsn)
    session = sessionmaker(bind=engine)()

    for view in reversed(all_views):
        view.drop(session)
    for view in all_views:
        view.create(session)
        if args.refresh and isinstance(view, MaterializedViewMixin):
            view.refresh(session)
