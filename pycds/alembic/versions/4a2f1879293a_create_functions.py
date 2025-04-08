"""Create functions

Revision ID: 4a2f1879293a
Revises: 522eed334c85
Create Date: 2020-01-28 16:43:12.112378

"""

from alembic import op
from pycds.context import get_su_role_name
from pycds.orm.functions.version_4a2f1879293a import (
    closest_stns_within_threshold,
    daily_ts,
    daysinmonth,
    do_query_one_station,
    effective_day,
    getstationvariabletable,
    lastdateofmonth,
    monthly_ts,
    query_one_station,
    query_one_station_climo,
    season,
    updatesdateedate,
)

# revision identifiers, used by Alembic.
revision = "4a2f1879293a"
down_revision = "522eed334c85"
branch_labels = None
depends_on = None


stored_procedures = (
    closest_stns_within_threshold,
    daily_ts,
    daysinmonth,
    do_query_one_station,
    effective_day,
    getstationvariabletable,
    lastdateofmonth,
    monthly_ts,
    query_one_station,
    query_one_station_climo,
    season,
    updatesdateedate,
)


def upgrade():
    # In order to create stored procedures using untrusted languages (we use
    # `plpython3u`), the user needs superuser privileges. This is achieved by
    # temporarily setting the role to a superuser role name that is externally
    # granted to the user only for the period when database migrations are
    # performed.
    op.set_role(get_su_role_name())
    for sp in stored_procedures:
        op.create_replaceable_object(sp)
    op.reset_role()


def downgrade():
    for sp in stored_procedures:
        op.drop_replaceable_object(sp)
