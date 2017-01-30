from pytest import mark

from sqlalchemy import text, inspect

import pycds
from pycds.manage_views import manage_views


base_view = ['discarded_obs_v']
daily_views = ['daily_max_temperature_mv', 'daily_min_temperature_mv']
monthly_views = ['monthly_average_of_daily_max_temperature_mv', 'monthly_average_of_daily_min_temperature_mv',
                 'monthly_total_precipitation_mv']


@mark.parametrize(', operation, view, view_names', [
    ('create', 'all', daily_views + monthly_views),
])
def test_it(engine, session, operation, view, view_names):
    table_names = inspect(engine).get_table_names()
    # print('before', table_names)
    for vn in view_names:
        assert vn not in table_names
    manage_views(session, operation, view)
    session.commit()
    table_names = inspect(engine).get_table_names()
    # print('after', table_names)
    for vn in view_names:
        assert vn in table_names
