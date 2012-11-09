from pycds import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from optparse import OptionParser

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-c', '--connection_string', dest='connection_string', help='PostgreSQL connection string')
    parser.set_defaults(connection_string='postgresql://hiebert@windy.pcic.uvic.ca/crmp?sslmode=require',)
    (opts, args) = parser.parse_args()

    engine = create_engine(opts.connection_string, echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    for net, in session.query(Network.name).order_by(Network.name):
        print net
