"""Create missing indexes

Revision ID: 0d99ba90c229
Revises: e688e520d265
Create Date: 2021-05-20 10:02:02.111616

Create indexes that are defined on ORM classes but which were not created
by earlier migrations.
"""
import logging
from alembic import op
import sqlalchemy as sa
from pycds import get_schema_name
from pycds.orm.tables import (
    ClimoObsCount,
    CollapsedVariables,
    History,
    Station,
    Variable,
    ClimatologyAttributes,
    ObsCountPerMonthHistory,
    StationObservationStats,
)


# revision identifiers, used by Alembic.
revision = "0d99ba90c229"
down_revision = "e688e520d265"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic")
schema_name = get_schema_name()

classes = (
    ClimoObsCount,
    CollapsedVariables,
    History,
    Station,
    Variable,
    ClimatologyAttributes,
    ObsCountPerMonthHistory,
    StationObservationStats,
)


def upgrade():
    for ORMClass in classes:
        logger.debug(f"Processing class {ORMClass.__name__}; table {ORMClass.__tablename__}")
        for index in ORMClass.__table__.indexes:
            logger.debug(
                f"Creating index '{index.name} on table {index.table.name}'"
            )
            op.create_index(
                index_name=index.name,
                table_name=index.table.name,
                columns=[col.name for col in index.columns],
                unique=index.unique,
                schema=schema_name,
            )


def downgrade():
    for ORMClass in classes:
        for index in ORMClass.__table__.indexes:
            logger.debug(
                f"Dropping index '{index.name} on table {index.table.name}'"
            )
            op.drop_index(
                index_name=index.name,
                table_name=index.table.name,
                schema=schema_name,
            )
