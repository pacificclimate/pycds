import sys
import logging
from optparse import OptionParser
from pkg_resources import resource_filename

from sqlalchemy import create_engine

from pycds.util import create_test_database, create_test_data
    
if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-d", "--dsn", dest="dsn", help="Source database DSN from which to read")
    parser.add_option("-t", "--testdsn", dest="test_dsn", help="Destination DSN to which to write")
    parser.set_defaults(dsn='postgresql://hiebert@monsoon.pcic/crmp?sslmode=require', testdsn='sqlite+pysqlite:////tmp/crmp.sqlite')
    opts, args = parser.parse_args()
                        
    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    read_engine = create_engine(opts.dsn)
    write_engine = create_engine(opts.testdsn)

    create_test_database(write_engine)
    create_test_data(write_engine)
