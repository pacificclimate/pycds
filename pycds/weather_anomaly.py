"""Weather anomaly views

Define views for weather anomaly applications using tools in pycds.view_helpers.

Materialized view: Daily maximum temperature (DailyMaxTemperature)
Materialized view: Daily minimum temperature (DailyMinTemperature)
  - These views support views that deliver monthly average of daily max/min temperature.
  - Observations flagged with meta_native_flag.discard or meta_pcic_flag.discard are not included in the view.
  - data_coverage is the fraction of observations actually available in a day relative to those potentially available
     in a day. The computation is correct for a given day if and only if the observation frequency does not change
     during that day. If such a change does occur, the data_coverage fraction for the day will be > 1, which is not
     fatal to distinguishing adequate coverage.
  - These views are defined with plain-text SQL queries instead of with SQLAlchemy select expressions.
      The SQL SELECT statements were already written, and the work required to translate them to SQLAlchemy seemed
      unnecessary. See https://docs.sqlalchemy.org/en/latest/core/tutorial.html#using-textual-sql
      This decision subject to revision.

Materialized View: Monthly average of daily maximum temperature (MonthlyAverageOfDailyMaxTemperature)
Materialized View: Monthly average of daily minimum temperature (MonthlyAverageOfDailyMinTemperature)
  - data_coverage is the fraction of of observations actually available in a month relative to those potentially
    available in a month, and is robust to varying reporting frequencies on different days in the month (but see
    caveat for daily data coverage above).
  - These views are defined with plain-text SQL queries instead of with SQLAlchemy select expressions.
      The SQL SELECT statements were already written, and the work required to translate them to SQLAlchemy seemed
      excessive and unnecessary. See https://docs.sqlalchemy.org/en/latest/core/tutorial.html#using-textual-sql

Materialized View: Monthly total precipitation (MonthlyTotalPrecipitation)
  - Observations flagged with meta_native_flag.discard or meta_pcic_flag.discard are not included in the view.
  - data_coverage is the fraction of observations actually available in a month relative to those potentially
     available in a month. This computation is correct if and only if the observation frequency does not change
     during any one day in the month. It remains approximately correct if such days are rare, and remains valid
     for the purpose of distinguishing adequate coverage.
  - This view is defined with plain-text SQL queries instead of with SQLAlchemy select expressions.
      The SQL SELECT statements were already written, and the work required to translate them to SQLAlchemy seemed
      excessive and unnecessary. See https://docs.sqlalchemy.org/en/latest/core/tutorial.html#using-textual-sql

Note: Schema name: See note on this topic in pycds/__init__.py

Note: Session-less queries: All queries defined in this module are session-less;
that is they are created using the `sqlalchemy.orm.query.Query` object instead
of via `Session.query()`. Such Query objects have no session attached, which
is required because the design of the manual materialized view implementation
delays association of the session to the matview until it is explicitly created
(i.e., Matview.create(session)).
"""

from sqlalchemy import MetaData, func, and_, not_, case
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Query

from pycds import \
    get_schema_name, \
    History, Obs, ObsRawNativeFlags, NativeFlag, ObsRawPCICFlags, PCICFlag, \
    Variable
from pycds.materialized_view_helpers import MaterializedViewMixin

Base = declarative_base(metadata=MetaData(schema=get_schema_name()))
metadata = Base.metadata


# Subquery used in daily temperature extrema and monthly total precip queries
# This query returns all Obs items with no `discard==True` flags attached.
good_obs = (
    Query([
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
    """Return a SQLAlchemy query for a specified extremum of daily temperature.

    Args:
        extremum (str): 'max' | 'min'

    Returns:
        sqlalchemy.sql.expression.Query
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
            func.sum(case({
                'daily': 1.0,
                '12-hourly': 0.5,
                '1-hourly': 1 / 24,
            }, value=History.freq)).label('data_coverage')
        ])
        .select_from(good_obs)
            .join(Variable)
            .join(History)
        .filter(Variable.standard_name == 'air_temperature')
        .filter(Variable.cell_method.in_(
            (f'time: {extremum}imum', 'time: point', 'time: mean')))
        .filter(History.freq.in_(('1-hourly', '12-hourly', 'daily')))
        .group_by(History.id, good_obs.c.vars_id, 'obs_day')
    )


class DailyMaxTemperature(Base, MaterializedViewMixin):
    __selectable__ = daily_temperature_extremum('max').selectable
    __primary_key__ = 'history_id vars_id obs_day'.split()


class DailyMinTemperature(Base, MaterializedViewMixin):
    __selectable__ = daily_temperature_extremum('min').selectable
    __primary_key__ = 'history_id vars_id obs_day'.split()


def monthly_average_of_daily_temperature_extremum_with_total_coverage(extremum):
    """Return a SQLAlchemy query for a monthly average of a specified
    extremum of daily temperature, with monthly total data coverage.

    Args:
        extremum (str): 'max' | 'min'

    Returns:
        sqlalchemy.sql.expression.Query
    """
    # TODO: Rename. Geez.

    DailyExtremeTemp = \
        DailyMaxTemperature if extremum == 'max' \
            else DailyMinTemperature

    obs_month = func.date_trunc('month', DailyExtremeTemp.obs_day)

    return (
        Query([
            DailyExtremeTemp.history_id.label('history_id'),
            DailyExtremeTemp.vars_id.label('vars_id'),
            obs_month.label('obs_month'),
            func.avg(DailyExtremeTemp.statistic).label('statistic'),
            func.sum(DailyExtremeTemp.data_coverage)
                .label('total_data_coverage'),
        ])
        .select_from(DailyExtremeTemp)
        .group_by(
            DailyExtremeTemp.history_id,
            DailyExtremeTemp.vars_id,
            'obs_month'
        )
    )


def monthly_average_of_daily_temperature_extremum_with_avg_coverage(extremum):
    """Return a SQLAlchemy query for a monthly average of a specified
    extremum of daily temperature, with monthly average data coverage.

    Args:
        extremum (str): 'max' | 'min'

    Returns:
        sqlalchemy.sql.expression.Query
    """
    # TODO: Rename. Geez.

    avg_daily_extreme_temperature = \
        monthly_average_of_daily_temperature_extremum_with_total_coverage(extremum)\
            .subquery('avg_daily_extreme_temperature')

    func_schema = getattr(func, get_schema_name())

    return (
        Query([
            avg_daily_extreme_temperature.c.history_id.label('history_id'),
            avg_daily_extreme_temperature.c.vars_id.label('vars_id'),
            avg_daily_extreme_temperature.c.obs_month.label('obs_month'),
            avg_daily_extreme_temperature.c.statistic.label('statistic'),
            (
                avg_daily_extreme_temperature.c.total_data_coverage /
                func_schema.DaysInMonth(avg_daily_extreme_temperature.c.obs_month)
            ).label('data_coverage')
        ])
        .select_from(avg_daily_extreme_temperature)
    )


class MonthlyAverageOfDailyMaxTemperature(Base, MaterializedViewMixin):
    __selectable__ = monthly_average_of_daily_temperature_extremum_with_avg_coverage('max').selectable
    __primary_key__ = 'history_id vars_id obs_month'.split()


class MonthlyAverageOfDailyMinTemperature(Base, MaterializedViewMixin):
    __selectable__ = monthly_average_of_daily_temperature_extremum_with_avg_coverage('min').selectable
    __primary_key__ = 'history_id vars_id obs_month'.split()


def monthly_total_precipitation_with_total_coverage():
    """Return a SQLAlchemy query for monthly total of precipitation,
    with monthly total data coverage.

    Returns:
        sqlalchemy.sql.expression.Query
    """
    return (
        Query([
            History.id.label('history_id'),
            good_obs.c.vars_id.label('vars_id'),
            func.date_trunc('month', good_obs.c.time).label('obs_month'),
            func.sum(good_obs.c.datum).label('statistic'),
            func.sum(case({
                'daily': 1.0,
                '12-hourly': 0.5,
                '1-hourly': 1 / 24,
            }, value=History.freq)).label('total_data_coverage')
        ])
        .select_from(good_obs)
            .join(History)
            .join(Variable)
        .filter(Variable.standard_name.in_((
            'lwe_thickness_of_precipitation_amount',
            'thickness_of_rainfall_amount',
            'thickness_of_snowfall_amount'
        )))
        .filter(Variable.cell_method == 'time: sum')
        .filter(History.freq.in_(('1-hourly', 'daily')))
        .group_by(History.id, good_obs.c.vars_id, 'obs_month')
    )


def monthly_total_precipitation_with_avg_coverage():
    """Return a SQLAlchemy query for monthly total precipitation,
    with monthly average data coverage.

    Returns:
        sqlalchemy.sql.expression.Query
    """
    # TODO: Rename. Geez.

    monthly_total_precip = \
        monthly_total_precipitation_with_total_coverage() \
            .subquery('monthly_total_precip')

    func_schema = getattr(func, get_schema_name())

    return (
        Query([
            monthly_total_precip.c.history_id.label('history_id'),
            monthly_total_precip.c.vars_id.label('vars_id'),
            monthly_total_precip.c.obs_month.label('obs_month'),
            monthly_total_precip.c.statistic.label('statistic'),
            (
                monthly_total_precip.c.total_data_coverage /
                func_schema.DaysInMonth(monthly_total_precip.c.obs_month)
            ).label('data_coverage')
        ])
        .select_from(monthly_total_precip)
    )


class MonthlyTotalPrecipitation(Base, MaterializedViewMixin):
    __selectable__ = monthly_total_precipitation_with_avg_coverage().selectable
    __primary_key__ = 'history_id vars_id obs_month'.split()
