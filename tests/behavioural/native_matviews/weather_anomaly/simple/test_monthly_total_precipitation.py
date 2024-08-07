from pycds.orm.native_matviews import (
    MonthlyTotalPrecipitation,
    monthly_total_precipitation_with_total_coverage,
    monthly_total_precipitation_with_avg_coverage,
)

import pytest


def check(results, months, variable):
    assert {r.vars_id for r in results} == {variable.id}
    assert {r.obs_month.month for r in results} == set(months)


@pytest.mark.slow
def test_monthly_total_precipitation_with_total_coverage_query(
    obs1_precip_sesh, obs1_months, var_precip_net1_1
):
    sesh = obs1_precip_sesh
    monthly_total_precip = (
        monthly_total_precipitation_with_total_coverage().with_session(sesh).all()
    )
    check(monthly_total_precip, obs1_months, var_precip_net1_1)


@pytest.mark.slow
def test_monthly_total_precipitation_with_avg_coverage_query(
    obs1_precip_sesh, obs1_months, var_precip_net1_1
):
    sesh = obs1_precip_sesh
    monthly_total_precip = (
        monthly_total_precipitation_with_avg_coverage().with_session(sesh).all()
    )
    check(monthly_total_precip, obs1_months, var_precip_net1_1)


@pytest.mark.slow
def test_monthly_total_precipitation_matview(
    obs1_precip_sesh, obs1_months, var_precip_net1_1
):
    sesh = obs1_precip_sesh
    sesh.execute(MonthlyTotalPrecipitation.refresh())
    monthly_total_precip = sesh.query(MonthlyTotalPrecipitation).all()
    check(monthly_total_precip, obs1_months, var_precip_net1_1)
