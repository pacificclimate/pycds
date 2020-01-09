import pytest

from pycds import \
    get_schema_name, \
    History, Obs, ObsRawNativeFlags, NativeFlag, ObsRawPCICFlags, PCICFlag, \
    Variable
from pycds.weather_anomaly import \
    good_obs, daily_temperature_extremum, \
    DailyMaxTemperature, DailyMinTemperature, \
    monthly_average_of_daily_temperature_extremum_with_total_coverage, \
    monthly_average_of_daily_temperature_extremum_with_avg_coverage, \
    MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, \
    monthly_total_precipitation_with_total_coverage, \
    monthly_total_precipitation_with_avg_coverage, \
    MonthlyTotalPrecipitation

from sqlalchemy.orm import Query
from sqlalchemy.sql import text, column
from sqlalchemy import MetaData, func, and_, not_, case



# TODO: Remove
def test_good_obs_defn():
    print('Query')
    print(good_obs.compile(compile_kwargs={"literal_binds": True}))
    print()
    # print ('text')
    # print(good_obs_text)


def test_good_obs(obs1_temp_sesh):
    sesh = obs1_temp_sesh
    observations = sesh.query(Obs)
    print('# observations', observations.count())
    goods = sesh.query(good_obs)
    print('# goods (Q)', goods.count())
    print(type(goods.first()))
    print(goods.first())
    print(goods.first().id)


# TODO: Remove
@pytest.mark.parametrize('extremum', ('max', 'min'))
def test_daily_extreme_temperature_defn(extremum):
    selectable = daily_temperature_extremum(extremum).selectable
    print(selectable.compile(compile_kwargs={"literal_binds": True}))


@pytest.mark.parametrize('extremum', ('max', 'min'))
def test_daily_temperature_extremum_query(
        extremum, obs1_temp_sesh, obs1_months, obs1_days, obs1_hours,
        var_temp_point
):
    sesh = obs1_temp_sesh
    daily_extreme_temps = \
        daily_temperature_extremum(extremum).with_session(sesh).all()
    # print('# daily_extreme_temps', len(daily_extreme_temps))
    # for row in daily_extreme_temps:
    #     print(row)
    # print('# variables')
    # for v in sesh.query(Variable).all():
    #     print(v)
    assert len(daily_extreme_temps) == len(obs1_months) * len(obs1_days)
    assert {r.obs_day.month for r in daily_extreme_temps} == set(obs1_months)
    assert {r.obs_day.day for r in daily_extreme_temps} == set(obs1_days)
    assert {r.vars_id for r in daily_extreme_temps} == {var_temp_point.id}
    ext_value = max(obs1_hours) if extremum == 'max' else min(obs1_hours)
    assert all(r.statistic == ext_value for r in daily_extreme_temps)


@pytest.mark.parametrize('Matview', (DailyMaxTemperature, DailyMinTemperature))
def test_daily_extreme_temperature_matview(
        Matview, obs1_temp_sesh, obs1_months, obs1_days, obs1_hours,
        var_temp_point
):
    sesh = obs1_temp_sesh
    Matview.refresh(sesh)
    daily_extreme_temps = sesh.query(Matview).all()
    # print('# daily_extreme_temps', len(daily_extreme_temps))
    # for row in daily_extreme_temps:
    #     print(row)
    # print('# variables')
    # for v in sesh.query(Variable).all():
    #     print(v)
    assert len(daily_extreme_temps) == len(obs1_months) * len(obs1_days)
    assert {r.obs_day.month for r in daily_extreme_temps} == set(obs1_months)
    assert {r.obs_day.day for r in daily_extreme_temps} == set(obs1_days)
    assert {r.vars_id for r in daily_extreme_temps} == {var_temp_point.id}
    ext_value = max(obs1_hours) if Matview == DailyMaxTemperature else min(obs1_hours)
    assert all(r.statistic == ext_value for r in daily_extreme_temps)


def test_functions(schema_name, obs1_temp_sesh):
    functions = obs1_temp_sesh.execute('''
        SELECT routines.routine_name, parameters.data_type, parameters.ordinal_position
        FROM information_schema.routines
            LEFT JOIN information_schema.parameters ON routines.specific_name=parameters.specific_name
        WHERE routines.specific_schema='crmp'
        ORDER BY routines.routine_name, parameters.ordinal_position;
    ''').fetchall()
    print('crmp functions')
    for f in functions:
        print(f)


@pytest.mark.parametrize('extremum', ('max', 'min'))
def test_monthly_average_of_daily_temperature_extremum_with_total_coverage_query(
        extremum, obs1_temp_sesh, obs1_months, var_temp_point
):
    sesh = obs1_temp_sesh
    DailyExtremeTemp = \
        DailyMaxTemperature if extremum == 'max' \
            else DailyMinTemperature
    DailyExtremeTemp.refresh(sesh)
    ma_daily_extreme_temps = \
        monthly_average_of_daily_temperature_extremum_with_total_coverage(extremum)\
            .with_session(sesh).all()
    print('# ma_daily_extreme_temps', len(ma_daily_extreme_temps))
    for row in ma_daily_extreme_temps:
        print(row)
    assert {r.vars_id for r in ma_daily_extreme_temps} == {var_temp_point.id}
    assert {r.obs_month.month for r in ma_daily_extreme_temps} == set(obs1_months)


@pytest.mark.parametrize('extremum', ('max', 'min'))
def test_monthly_average_of_daily_temperature_extremum_with_avg_coverage_query(
        extremum, obs1_temp_sesh, obs1_months, var_temp_point
):
    sesh = obs1_temp_sesh
    DailyExtremeTemp = \
        DailyMaxTemperature if extremum == 'max' \
            else DailyMinTemperature
    DailyExtremeTemp.refresh(sesh)
    ma_daily_extreme_temps = \
        monthly_average_of_daily_temperature_extremum_with_avg_coverage(extremum)\
            .with_session(sesh).all()
    print('# ma_daily_extreme_temps', len(ma_daily_extreme_temps))
    for row in ma_daily_extreme_temps:
        print(row)
    assert {r.vars_id for r in ma_daily_extreme_temps} == {var_temp_point.id}
    assert {r.obs_month.month for r in ma_daily_extreme_temps} == set(obs1_months)


@pytest.mark.parametrize('Matview', (MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature))
def test_monthly_average_of_daily_extreme_temperature_matview(
        Matview, obs1_temp_sesh, obs1_months, var_temp_point
):
    sesh = obs1_temp_sesh
    DailyExtremeTemp = \
        DailyMaxTemperature if Matview == MonthlyAverageOfDailyMaxTemperature \
            else DailyMinTemperature
    DailyExtremeTemp.refresh(sesh)
    Matview.refresh(sesh)
    ma_daily_extreme_temps = sesh.query(Matview).all()
    print('# ma_daily_extreme_temps', len(ma_daily_extreme_temps))
    for row in ma_daily_extreme_temps:
        print(row)
    assert {r.vars_id for r in ma_daily_extreme_temps} == {var_temp_point.id}
    assert {r.obs_month.month for r in ma_daily_extreme_temps} == set(obs1_months)


def test_monthly_total_precipitation_with_total_coverage_query(
        obs1_precip_sesh, obs1_months, var_precip_net1_1
):
    sesh = obs1_precip_sesh
    monthly_total_precip = \
        monthly_total_precipitation_with_total_coverage() \
            .with_session(sesh).all()
    print('# monthly_total_precip', len(monthly_total_precip))
    for row in monthly_total_precip:
        print(row)
    assert {r.vars_id for r in monthly_total_precip} == {var_precip_net1_1.id}
    assert {r.obs_month.month for r in monthly_total_precip} == set(obs1_months)


def test_monthly_total_precipitation_with_avg_coverage_query(
        obs1_precip_sesh, obs1_months, var_precip_net1_1
):
    sesh = obs1_precip_sesh
    monthly_total_precip = \
        monthly_total_precipitation_with_avg_coverage() \
            .with_session(sesh).all()
    print('# monthly_total_precip', len(monthly_total_precip))
    for row in monthly_total_precip:
        print(row)
    assert {r.vars_id for r in monthly_total_precip} == {var_precip_net1_1.id}
    assert {r.obs_month.month for r in monthly_total_precip} == set(obs1_months)


def test_monthly_total_precipitation_matview(
        obs1_precip_sesh, obs1_months, var_precip_net1_1
):
    sesh = obs1_precip_sesh
    MonthlyTotalPrecipitation.refresh(sesh)
    monthly_total_precip = sesh.query(MonthlyTotalPrecipitation).all()
    print('# monthly_total_precip', len(monthly_total_precip))
    for row in monthly_total_precip:
        print(row)
    assert {r.vars_id for r in monthly_total_precip} == {var_precip_net1_1.id}
    assert {r.obs_month.month for r in monthly_total_precip} == set(obs1_months)
