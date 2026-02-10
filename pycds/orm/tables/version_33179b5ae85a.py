"""
ORM table definitions at migration version 33179b5ae85a.

This version includes the network_display_name column added to Network and NetworkHistory tables.
All other tables are imported from base.py since they haven't changed at this revision.
"""

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    String,
    ForeignKey,
    func,
)
from sqlalchemy.sql import literal_column
from sqlalchemy.orm import relationship, synonym

from pycds.alembic.change_history_utils import hx_table_name

# Import Base and all other tables from base module
from .base import (
    Base,
    metadata,
    no_newline_ck_name,
    no_newline_ck_check,
    Contact,
    Station,
    StationHistory,
    History,
    HistoryHistory,
    ObsRawNativeFlags,
    ObsRawPCICFlags,
    MetaSensor,
    Obs,
    ObsHistory,
    TimeBound,
    Variable,
    VariableHistory,
    NativeFlag,
    PCICFlag,
    DerivedValue,
)


class Network(Base):
    """Network table ORM at revision 33179b5ae85a"""

    __tablename__ = "meta_network"

    # Columns - order must match physical column order in database after migrations
    # The network_display_name column was added at the END via ALTER TABLE ADD COLUMN
    id = Column("network_id", Integer, primary_key=True)
    name = Column("network_name", String)
    long_name = Column("description", String)
    virtual = Column(String(255))
    publish = Column(Boolean)
    color = Column("col_hex", String)
    contact_id = Column(Integer, ForeignKey("meta_contact.contact_id"))
    mod_time = Column(DateTime, nullable=False, server_default=func.now())
    mod_user = Column(
        String(64), nullable=False, server_default=literal_column("current_user")
    )
    display_name = Column("network_display_name", String, unique=True)  # Added at end by ALTER TABLE

    # Relationships
    stations = relationship(
        "Station",
        order_by="Station.id",
        back_populates="network",
        cascade_backrefs=False,
    )
    meta_station = synonym("stations")
    variables = relationship(
        "Variable", back_populates="network", cascade_backrefs=False
    )
    meta_vars = synonym("variables")
    contact = relationship("Contact", back_populates="networks", cascade_backrefs=False)
    meta_contact = synonym("contact")
    native_flags = relationship(
        "NativeFlag",
        order_by="NativeFlag.id",
        back_populates="network",
        cascade_backrefs=False,
    )
    meta_native_flag = synonym("native_flags")

    def __str__(self):
        return f"<CRMP Network {self.name}>"



class NetworkHistory(Base):
    """NetworkHistory table ORM at revision"""

    __tablename__ = hx_table_name("meta_network", schema=None)

    # Columns - order must match physical column order in database after migrations
    # The network_display_name column was added at the END via ALTER TABLE ADD COLUMN
    network_id = Column(Integer, nullable=False, index=True)
    name = Column("network_name", String)
    long_name = Column("description", String)
    virtual = Column(String(255))
    publish = Column(Boolean)
    color = Column("col_hex", String)
    contact_id = Column(Integer)
    mod_time = Column(DateTime, nullable=False, server_default=func.now())
    mod_user = Column(
        String(64), nullable=False, server_default=literal_column("current_user")
    )
    display_name = Column("network_display_name", String)  # Added at end by ALTER TABLE
    deleted = Column(Boolean, default=False)
    meta_network_hx_id = Column(Integer, primary_key=True)

    def __str__(self):
        return f"<CRMP NetworkHistory {self.name}>"


# import other tables from base
