import pytest
from pycds.orm.native_matviews import (
    DailyMaxTemperature,
    DailyMinTemperature,
    monthly_average_of_daily_temperature_extremum_with_total_coverage,
    monthly_average_of_daily_temperature_extremum_with_avg_coverage,
    MonthlyAverageOfDailyMaxTemperature,
    MonthlyAverageOfDailyMinTemperature,
)


DailyExtremeTemps = {"max": DailyMaxTemperature, "min": DailyMinTemperature}

MonthlyAverageOfDailyExtremeTemps = {
    "max": MonthlyAverageOfDailyMaxTemperature,
    "min": MonthlyAverageOfDailyMinTemperature,
}


def check(results, months, variable):
    assert {r.vars_id for r in results} == {variable.id}
    assert {r.obs_month.month for r in results} == set(months)


@pytest.mark.slow
@pytest.mark.parametrize("extremum", ("max", "min"))
def test_monthly_average_of_daily_temperature_extremum_with_total_coverage_query(
    extremum, obs1_temp_sesh, obs1_months, var_temp_point
):
    sesh = obs1_temp_sesh
    DailyExtremeTemp = DailyExtremeTemps[extremum]
    sesh.execute(DailyExtremeTemp.refresh())
    ma_daily_extreme_temps = (
        monthly_average_of_daily_temperature_extremum_with_total_coverage(extremum)
        .with_session(sesh)
        .all()
    )
    check(ma_daily_extreme_temps, obs1_months, var_temp_point)


@pytest.mark.slow
@pytest.mark.parametrize("extremum", ("max", "min"))
def test_monthly_average_of_daily_temperature_extremum_with_avg_coverage_query(
    extremum, obs1_temp_sesh, obs1_months, var_temp_point
):
    sesh = obs1_temp_sesh
    DailyExtremeTemp = DailyExtremeTemps[extremum]
    sesh.execute(DailyExtremeTemp.refresh())
    ma_daily_extreme_temps = (
        monthly_average_of_daily_temperature_extremum_with_avg_coverage(extremum)
        .with_session(sesh)
        .all()
    )
    check(ma_daily_extreme_temps, obs1_months, var_temp_point)


@pytest.mark.slow
@pytest.mark.parametrize("extremum", ("max", "min"))
def test_monthly_average_of_daily_extreme_temperature_matview(
    extremum, obs1_temp_sesh, obs1_months, var_temp_point
):
    sesh = obs1_temp_sesh
    DailyExtremeTemp = DailyExtremeTemps[extremum]
    sesh.execute(DailyExtremeTemp.refresh())
    MonthlyAverageOfDailyExtremeTemp = MonthlyAverageOfDailyExtremeTemps[extremum]
    sesh.execute(MonthlyAverageOfDailyExtremeTemp.refresh())
    ma_daily_extreme_temps = sesh.query(MonthlyAverageOfDailyExtremeTemp).all()
    check(ma_daily_extreme_temps, obs1_months, var_temp_point)
