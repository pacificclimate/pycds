"""convert monthly_total_precipitation_mv to native matview

Revision ID: 081f17262852
Revises: 3505750d3416
Create Date: 2024-04-23 13:39:39.618612

"""

import logging
from alembic import op
import sqlalchemy as sa
from pycds.alembic.util import (
    create_matview,
    drop_matview,
    grant_standard_table_privileges,
)
from pycds import get_schema_name, get_su_role_name

from pycds.database import matview_exists

from pycds.orm.native_matviews.version_081f17262852 import MonthlyTotalPrecipitation
from pycds.orm.manual_matviews.version_8fd8f556c548 import (
    MonthlyTotalPrecipitation as OldMonthlyTotalPrecipitation,
)
from pycds.orm.native_matviews.version_081f17262852 import DailyMaxTemperature
from pycds.orm.manual_matviews.version_8fd8f556c548 import (
    DailyMaxTemperature as OldDailyMaxTemperature,
)
from pycds.orm.native_matviews.version_081f17262852 import DailyMinTemperature
from pycds.orm.manual_matviews.version_8fd8f556c548 import (
    DailyMinTemperature as OldDailyMinTemperature,
)
from pycds.orm.native_matviews.version_081f17262852 import (
    MonthlyAverageOfDailyMaxTemperature,
)
from pycds.orm.manual_matviews.version_8fd8f556c548 import (
    MonthlyAverageOfDailyMaxTemperature as OldMonthlyAverageOfDailyMaxTemperature,
)
from pycds.orm.native_matviews.version_081f17262852 import (
    MonthlyAverageOfDailyMinTemperature,
)
from pycds.orm.manual_matviews.version_8fd8f556c548 import (
    MonthlyAverageOfDailyMinTemperature as OldMonthlyAverageOfDailyMinTemperature,
)

# note that the last two matviews depend on the previous two matviews, so
# this is the correct order to create the views, but they need to be dropped
# in reverse order.
native_managed_matviews = [
    (MonthlyTotalPrecipitation, OldMonthlyTotalPrecipitation),
    (DailyMaxTemperature, OldDailyMaxTemperature),
    (DailyMinTemperature, OldDailyMinTemperature),
    (MonthlyAverageOfDailyMaxTemperature, OldMonthlyAverageOfDailyMaxTemperature),
    (MonthlyAverageOfDailyMinTemperature, OldMonthlyAverageOfDailyMinTemperature),
]

# revision identifiers, used by Alembic.
revision = "081f17262852"
down_revision = "3505750d3416"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic")
schema_name = get_schema_name()


def upgrade():
    op.set_role(get_su_role_name())
    engine = op.get_bind().engine

    for native, managed in native_managed_matviews:
        if matview_exists(engine, native.__tablename__, schema=schema_name):
            logger.info(
                f"A native materialized view '{native.__tablename__}' "
                f"already exists in the database; skipping upgrade"
            )

        else:
            # drop old "matview"-style table
            op.drop_replaceable_object(managed)

            # Replace with native matview
            create_matview(native, schema=schema_name)

    op.reset_role()


def downgrade():
    op.set_role(get_su_role_name())
    for native, managed in reversed(native_managed_matviews):
        # Drop native matview
        drop_matview(native, schema=schema_name)

        op.create_replaceable_object(managed)

        grant_standard_table_privileges(managed.__tablename__, schema=schema_name)

    op.reset_role()
