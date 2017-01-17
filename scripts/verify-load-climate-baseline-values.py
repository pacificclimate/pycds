import sys
import logging
from argparse import ArgumentParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pycds.climate_baseline_helpers import verify_baseline_network_and_variables

if __name__ == '__main__':
    parser = ArgumentParser(description="""Script to verify loading of climate baseline values for a single variable into a database.
Loading is presumed to have occurred from the following files::

/var/run/user/1000/gvfs/smb-share\:server\=pcic-storage.pcic.uvic.ca\,share\=storage/data/projects/PRISM/bc_climate/bc_7100_finals/station_data/tmp/bc_tmax_7100.lst
/var/run/user/1000/gvfs/smb-share\:server\=pcic-storage.pcic.uvic.ca\,share\=storage/data/projects/PRISM/bc_climate/bc_7100_finals/station_data/tmp/bc_tmin_7100.lst
/var/run/user/1000/gvfs/smb-share\:server\=pcic-storage.pcic.uvic.ca\,share\=storage/data/projects/PRISM/bc_climate/bc_7100_finals/station_data/ppt/bc_ppt_7100.lst

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
    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    engine = create_engine(args.dsn)
    session = sessionmaker(bind=engine)()

    assert False

    # verify_baseline_network_and_variables(session)