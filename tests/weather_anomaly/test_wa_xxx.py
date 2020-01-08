import pytest

from pycds import \
    get_schema_name, \
    History, Obs, ObsRawNativeFlags, NativeFlag, ObsRawPCICFlags, PCICFlag, \
    Variable
# from pycds.weather_anomaly import \
#     good_obs

from sqlalchemy.orm import Query
from sqlalchemy.sql import text, column
from sqlalchemy import MetaData, func, and_, not_, case


good_obs = (
    Query([
        # Obs
        Obs.id.label('id'),
        Obs.time.label('time'),
        Obs.mod_time.label('mod_time'),
        Obs.datum.label('datum'),
        Obs.vars_id.label('vars_id'),
        Obs.history_id.label('history_id'),
    ])
    .select_from(Obs)
        .outerjoin(ObsRawNativeFlags)
        .outerjoin(NativeFlag)
        .outerjoin(ObsRawPCICFlags)
        .outerjoin(PCICFlag)
    .group_by(Obs.id)
    .having(
        and_(
            not_(func.bool_or(func.coalesce(NativeFlag.discard, False))),
            not_(func.bool_or(func.coalesce(PCICFlag.discard, False))),
        )
    )
).subquery('good_obs')


def daily_temperature_extremum(extremum):
    """Return a SQLAlchemy selector for a specified extremum of daily
    temperature.

    Args:
        extremum (str): 'max' | 'min'

    Returns:
        sqlalchemy.sql.expression.FromClause
    """
    extremum_func = getattr(func, extremum)
    func_schema = getattr(func, get_schema_name())
    return (
        Query([
            History.id.label('history_id'),
            good_obs.c.vars_id.label('vars_id'),
            func_schema.effective_day(good_obs.c.time, extremum, History.freq)
                .label('obs_day'),
            extremum_func(good_obs.c.datum).label('statistic'),
            func.sum(
                case({
                    'daily': 1.0,
                    '12-hourly': 0.5,
                    '1-hourly': 1/24,
                },
                value=History.freq)
            ).label('data_coverage')
        ])
        .select_from(good_obs)
            .join(Variable)
            .join(History)
        .filter(Variable.standard_name == 'air_temperature')
        .filter(Variable.cell_method.in_(
            ('time: {0}imum'.format(extremum), 'time: point', 'time: mean')))
        .filter(History.freq.in_(('1-hourly', '12-hourly', 'daily')))
        .group_by(History.id, good_obs.c.vars_id, 'obs_day')
    )


# TODO: Remove
# def test_good_obs_defn():
#     print('Query')
#     print(good_obs.compile(compile_kwargs={"literal_binds": True}))
#     print()
#     # print ('text')
#     # print(good_obs_text)


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
# @pytest.mark.parametrize('extremum', ('max', 'min'))
# def test_daily_extreme_temperature_defn(extremum):
#     selectable = daily_temperature_extremum(extremum).selectable
#     print(selectable.compile(compile_kwargs={"literal_binds": True}))


@pytest.mark.parametrize('extremum', ('max', 'min'))
def test_daily_extreme_temperature(extremum, obs1_temp_sesh, obs1_days):
    sesh = obs1_temp_sesh
    thing = daily_temperature_extremum(extremum).subquery()
    daily_extreme_temps = sesh.query(thing)
    print('# daily_extreme_temps', daily_extreme_temps.count())
    for row in daily_extreme_temps:
        print(row)
    print('# variables')
    for v in sesh.query(Variable).all():
        print(v)
    assert daily_extreme_temps.count() == len(obs1_days)
    assert daily_extreme_temps.group_by('vars_id').join(Variable)


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


