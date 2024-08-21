from sqlalchemy import (
    Column,
    Boolean,
    Integer,
    String,
    ForeignKey,
    select,
    DateTime,
)
from sqlalchemy.orm import relationship, synonym

from pycds.alembic.extensions.replaceable_objects import ReplaceableView
from pycds.orm.tables import NetworkHistory
from pycds.orm.view_base import make_declarative_base


Base = make_declarative_base()


class Network(Base, ReplaceableView):
    """
    This object maps to the view which represents various networks (a.k.a., partners)
    providing data for the Climate Related Monitoring Program. There is one
    network row for each data provider, typically a BC Ministry, crown
    corporation or private company.

    Note: __selectable__ must order columns exactly as corresponding history table,
    NetworkHistory, does
    """

    __tablename__ = "meta_network"

    create_time = Column(DateTime)
    creator = Column(String)
    id = Column("network_id", Integer, primary_key=True)
    name = Column("network_name", String)
    long_name = Column("description", String)
    virtual = Column(String(255))
    publish = Column(Boolean)
    color = Column("col_hex", String)
    contact_id = Column(Integer, ForeignKey("meta_contact.contact_id"))

    # TODO: How to manage relationships amongst views??
    # stations = relationship("Station", order_by="Station.id", back_populates="network")
    # meta_station = synonym("stations")
    # variables = relationship("Variable", back_populates="network")
    # meta_vars = synonym("variables")
    # contact = relationship("Contact", back_populates="networks")
    # meta_contact = synonym("contact")  # Retain backwards compatibility
    # native_flags = relationship(
    #     "NativeFlag", order_by="NativeFlag.id", back_populates="network"
    # )
    # meta_native_flag = synonym("native_flags")  # Retain backwards compatibility

    __selectable__ = (
        select(
            NetworkHistory.create_time.label("mod_time"),
            NetworkHistory.creator.label("creator"),
            NetworkHistory.id.label("id"),
            NetworkHistory.name.label("name"),
            NetworkHistory.long_name.label("long_name"),
            NetworkHistory.virtual.label("virtual"),
            NetworkHistory.publish.label("publish"),
            NetworkHistory.color.label("color"),
            NetworkHistory.contact_id.label("contact_id"),
        )
        .distinct(NetworkHistory.id)
        .where(
            NetworkHistory.id.not_in(
                select(NetworkHistory.id).where(~NetworkHistory.deleted).subquery()
            )
        )
        .order_by(NetworkHistory.id.desc(), NetworkHistory.create_time.desc())
    )

    def __str__(self):
        return f"<CRMP Network {self.name}>"
