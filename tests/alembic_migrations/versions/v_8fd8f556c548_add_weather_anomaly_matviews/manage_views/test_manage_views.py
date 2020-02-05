import datetime

from pytest import mark

from pycds import Obs
from pycds.manage_views import daily_views, monthly_views, manage_views


daily_view_names = [v.base_viewname() for v in daily_views]
monthly_view_names = [v.base_viewname() for v in monthly_views]


@mark.parametrize(
    "what, exp_views",
    [
        ("daily", daily_views),
        # `refresh monthly-only` will fail if the daily views haven't been refreshed, succeed if they do;
        # we don't bother setting up the test machinery to test that
        ("all", daily_views + monthly_views),
    ],
)
def test_refresh(
    what,
    exp_views,
    prepared_sesh_left,
    network1,
    station1,
    history_stn1_hourly,
    var_temp_point,
    var_precip_net1_1,
):

    # Initially, each view should be empty, because no data
    for view in exp_views:
        assert prepared_sesh_left.query(view).count() == 0

    # Add observations (and all the equipment necessary therefor) so that
    # the views will have something to chew
    session = prepared_sesh_left
    session.add(network1)
    session.add(station1)
    session.add(history_stn1_hourly)
    session.add(var_temp_point)
    session.add_all(
        [
            Obs(
                variable=variable,
                history=history_stn1_hourly,
                time=datetime.datetime(2000, 1, 1, 1),
                datum=1.0,
            )
            for variable in [var_temp_point, var_precip_net1_1]
        ]
    )
    session.flush()

    # Chew
    manage_views(prepared_sesh_left, "refresh", what)
    prepared_sesh_left.flush()

    # Now each view should contain something
    for view in exp_views:
        assert prepared_sesh_left.query(view).count() > 0
