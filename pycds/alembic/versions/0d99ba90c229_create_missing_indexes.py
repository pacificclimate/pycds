"""Create missing indexes

Revision ID: 0d99ba90c229
Revises: e688e520d265
Create Date: 2021-05-20 10:02:02.111616

Create indexes that are defined on ORM classes but which were not created
by earlier migrations.
"""

import logging
from alembic import op
from pycds.context import get_schema_name

# WARNING: This migration uses a mixture of genuine tables and tables mapped to
# externally managed matviews (i.e., tables that are managed to provide a matview
# like behaviour but which are not simply tables). Eventually all externally managed
# matviews will be brought under management by PyCDS, but some have not yet been.
#
# This poses a problem (which actually exists for all types of table objects):
# The definitions of the indexes on any given table may change with future migrations.
# We must be careful to pluck the definitions from the earliest revision in which
# the table actually becomes a matview. (This also ought to be done for true tables,
# but we do not manage per-revision definitions of them, which, it appears by this
# argument, we ought.)
#
# Whenever an externally managed matview table mapping is moved into management by
# PyCDS, its definition needs to be updated. See ClimoObsCount below for an example.
from pycds import (
    CollapsedVariables,
    History,
    Station,
    Variable,
    ObsCountPerMonthHistory,
    StationObservationStats,
)
from pycds.orm.native_matviews.version_96729d6db8b3 import ClimoObsCount
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "0d99ba90c229"
down_revision = "2914c6c8a7f9"
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
    ObsCountPerMonthHistory,
    StationObservationStats,
)

def upgrade():
    for ORMClass in classes:
        logger.debug(
            f"Processing class {ORMClass.__name__}; table {ORMClass.__tablename__}"
        )
        for index in ORMClass.__table__.indexes:
            logger.debug(f"Creating index '{index.name} on table {index.table.name}'")
            op.create_index(
                index_name=index.name,
                table_name=index.table.name,
                columns=[col.name for col in index.columns],
                unique=index.unique,
                schema=schema_name,
                if_not_exists=True,
            )


def downgrade():
    for ORMClass in classes:
        for index in ORMClass.__table__.indexes:
            logger.debug(f"Dropping index '{index.name} on table {index.table.name}'")
            
            # Check if the index exists before attempting to drop it
            # This is necessary because the index may not have been created
            # in the first place, or it may have been dropped in a previous migration.
        
            # Only drop the index if it exists
            op.drop_index(
                index_name=index.name,
                table_name=index.table.name,
                schema=schema_name,
                if_exists=True,
            )
