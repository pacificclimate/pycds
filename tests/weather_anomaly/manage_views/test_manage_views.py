import datetime

from pytest import mark

from sqlalchemy import text, inspect

from pycds import Obs
from pycds.manage_views import daily_views, monthly_views, manage_views


daily_view_names = [v.viewname() for v in daily_views]
monthly_view_names = [v.viewname() for v in monthly_views]


@mark.parametrize('what, exp_matview_names', [
    ('daily', daily_view_names),
    # `create monthly-only` will fail if the daily views do not already exist, succeed if they do;
    # we don't bother setting up the test machinery to test that
    ('all', daily_view_names + monthly_view_names),
])
def test_create(per_test_engine, per_test_session, what, exp_matview_names):

    # print('>>>> search_path', [r.search_path for r in per_test_session.execute('SHOW search_path')])

    def check_views_and_tables(present):
        def check(expected_names, actual_names):
            for name in expected_names:
                assert (present and (name in actual_names)) or (not present and (name not in actual_names))

        check(exp_matview_names, inspect(per_test_engine).get_table_names(schema='crmp'))

    check_views_and_tables(False)
    manage_views(per_test_session, 'create', what)
    per_test_session.commit()
    check_views_and_tables(True)


@mark.parametrize('what, exp_views', [
    ('daily', daily_views),
    # `refresh monthly-only` will fail if the daily views haven't been refreshed, succeed if they do;
    # we don't bother setting up the test machinery to test that
    ('all', daily_views + monthly_views),
])
def test_refresh(sesh_with_views, what, exp_views,
                 network1, station1, history_stn1_hourly, var_temp_point, var_precip_net1_1):

    # initially, each view should be empty, because no data
    for view in exp_views:
        assert sesh_with_views.query(view).count() == 0

    # Add observations (and all the equipment necessary therefor) so that the views will have something to chew
    session = sesh_with_views
    session.add(network1)
    session.add(station1)
    session.add(history_stn1_hourly)
    session.add(var_temp_point)
    session.add_all([
        Obs(
            variable=variable,
            history=history_stn1_hourly,
            time=datetime.datetime(2000, 1, 1, 1),
            datum = 1.0
        )
        for variable in [var_temp_point, var_precip_net1_1]
    ])
    session.flush()

    # chew
    manage_views(sesh_with_views, 'refresh', what)
    sesh_with_views.flush()

    # now each view should contain something
    for view in exp_views:
        assert sesh_with_views.query(view).count() > 0
