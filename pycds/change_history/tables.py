"""
This ORM defines the change history tracking tables. Since "history" is already used
in PyCDS, and "change history" is a longish phrase, we have adopted the abbreviation
"cxhx".

We don't need to give the change history tables different names, since they are in a
different schema, but doing it this way has some advantages:

- When the search path includes both schemas there is no confusion possible.
- PyCDS can export all ORM objects without qualification.
- Queries are more compact and more readable.

Seems like a win. Can be reconsidered if there prove to be downsides.
"""

from sqlalchemy import MetaData, Column, BigInteger
from sqlalchemy.ext.declarative import declarative_base

from pycds.context import get_cxhx_schema_name
from pycds.orm.tables import Obs


Base = declarative_base(metadata=MetaData(schema=get_cxhx_schema_name()))
metadata = Base.metadata


class ObsCxhx(Base):
    __tablename__ = "obs_raw_cxhx"

    # Columns unique to cxhx
    id = Column("obs_raw_cxhx_id", BigInteger, primary_key=True)

    # Columns copied from Obs when a cxhx record is created.
    obs_raw_id = Obs.id
    mod_time = Obs.mod_time
    # mod_user, mod_reason, if added to Obs
    time = Obs.time
    datum = Obs.datum
    # vars_id?
    # history_id?
