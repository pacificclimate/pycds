"""
This ORM defines the change history tracking tables. Since "history" is already used
in PyCDS, and "change history" is a longish phrase, we have adopted the abbreviation
"cxhx".
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
