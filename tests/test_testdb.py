from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pycds.util import create_test_database, create_test_data
from pycds import Contact

def test_can_create_test_db():
    engine = create_engine('sqlite://')
    create_test_database(engine)
    create_test_data(engine)
    # Get some data
    sesh = sessionmaker(bind=engine)()
    q = sesh.query(Contact)
    assert len(q.all()) == 2
    
