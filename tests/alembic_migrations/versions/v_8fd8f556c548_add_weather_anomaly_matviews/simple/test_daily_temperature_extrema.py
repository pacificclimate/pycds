import pytest

from pycds.weather_anomaly.version_8fd8f556c548 import (
    daily_temperature_extremum,
    DailyMaxTemperature,
    DailyMinTemperature,
)


def check(results, extremum, months, days, hours, variable):
    assert len(results) == len(months) * len(days)
    assert {r.obs_day.month for r in results} == set(months)
    assert {r.obs_day.day for r in results} == set(days)
    assert {r.vars_id for r in results} == {variable.id}
    ext_value = max(hours) if extremum == "max" else min(hours)
    assert all(r.statistic == ext_value for r in results)


@pytest.mark.parametrize("extremum", ("max", "min"))
def test_daily_temperature_extremum_query(
    extremum,
    obs1_temp_sesh,
    obs1_months,
    obs1_days,
    obs1_hours,
    var_temp_point,
):
    sesh = obs1_temp_sesh
    daily_extreme_temps = (
        daily_temperature_extremum(extremum).with_session(sesh).all()
    )
    check(
        daily_extreme_temps,
        extremum,
        obs1_months,
        obs1_days,
        obs1_hours,
        var_temp_point,
    )


@pytest.mark.parametrize("extremum", ("max", "min"))
def test_daily_extreme_temperature_matview(
    extremum,
    obs1_temp_sesh,
    obs1_months,
    obs1_days,
    obs1_hours,
    var_temp_point,
):
    sesh = obs1_temp_sesh
    Matview = DailyMaxTemperature if extremum == "max" else DailyMinTemperature
    Matview.refresh(sesh)
    daily_extreme_temps = sesh.query(Matview).all()
    check(
        daily_extreme_temps,
        extremum,
        obs1_months,
        obs1_days,
        obs1_hours,
        var_temp_point,
    )
