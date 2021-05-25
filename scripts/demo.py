#! /usr/bin/env python

from pycds import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_, or_
from argparse import ArgumentParser

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "-c",
        "--connection_string",
        required=True,
        help="PostgreSQL connection string of form:\n\tdialect+driver://username:password@host:port/database\nExamples:\n\tpostgresql://scott:tiger@localhost/mydatabase\n\tpostgresql+psycopg2://scott:tiger@localhost/mydatabase\n\tpostgresql+pg8000://scott:tiger@localhost/mydatabase",
    )
    args = parser.parse_args()

    engine = create_engine(args.connection_string, echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Good tutorial information here: http://docs.sqlalchemy.org/en/latest/orm/tutorial.html

    # Print all Network Names
    print("\nPrint all network names")
    for (net,) in session.query(Network.name).order_by(Network.name):
        print(net)

    # Print the first 10 stations in the EC network when ordered ascencding
    print("\nFirst 10 stations in the EC network ordered ascending")
    for station in (
        session.query(Station)
        .filter(Network.name == "EC")
        .order_by(Station.native_id.asc())[:10]
    ):
        print(station.native_id)

    # Count the observations for station 1010066 in the EC network
    # *** Must use explicit query joins here or sqlalchemy creates ridculous crossjoins and subqueries
    print("\nObservation count for station 1010066 in EC network")
    print(
        session.query(Obs)
        .join(History)
        .join(Station)
        .join(Network)
        .filter(Network.name == "EC")
        .filter(Station.native_id == "1010066")
        .count()
    )

    # Find all history_ids for station 1010066 in the EC network using filter criteria
    print("\nFind all history ids representing EC native_id 1010066")
    for (hist,) in (
        session.query(History.id)
        .filter(History.station_id == Station.id)
        .filter(and_(Station.native_id == "1010066", Network.name == "EC"))
    ):
        print(hist)

    # Same thing with joins... better practice
    print(
        "\nFind all history ids representing EC native_id 1010066 using explicit joins"
    )
    for (hist,) in (
        session.query(History.id)
        .join(Station)
        .join(Network)
        .filter(Network.name == "EC")
        .filter(Station.native_id == "1010066")
    ):
        print(hist)
