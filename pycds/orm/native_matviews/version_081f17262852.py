"""
Native matviews for  weather anomaly application.

`MonthlyTotalPrecipitation`
  - Observations flagged with `meta_native_flag.discard` or
    `meta_pcic_flag.discard` are not included in the view.
  - `data_coverage` is the fraction of observations actually available in a
    month relative to those potentially available in a month. This computation
    is correct if and only if the observation frequency does not change
    during any one day in the month. It remains approximately correct if such
    days are rare, and remains valid for the purpose of distinguishing adequate
    coverage.
    
`DailyMaxTemperature`
`DailyMinTemperature`
  - These views support views that deliver monthly average of daily max/min
    temperature.
  - Observations flagged with `meta_native_flag.discard` or
    `meta_pcic_flag.discard` are not included in the view.
  - `data_coverage` is the fraction of observations actually available in a
     day relative to those potentially available in a day. The computation is
     correct for a given day if and only if the observation frequency does not
     change during that day. If such a change does occur, the `data_coverage`
     fraction for the day will be > 1, which is not fatal to distinguishing
     adequate coverage.

`MonthlyAverageOfDailyMaxTemperature`
`MonthlyAverageOfDailyMinTemperature`
  - `data_coverage` is the fraction of of observations actually available in
    a month relative to those potentially available in a month, and is robust
    to varying reporting frequencies on different days in the month (but see
    caveat for daily data coverage above).

Notes:
  - Schema name: See note on this topic in pycds/__init__.py
  
  - The variable with the network_variable_name cum_pcpn_amt is explicitly excluded
    by name from the MonthlyTotalPrecipitation matview. This variable represents 
    the amount of cumulative precipitation as recorded in a large open-ended cylinder
    and is prone to errors due to physical considerations like evaportaion and snow caps 
    blocking the cylinder. It has been determined that while this variable can be useful
    for spot-checking conditions "on the ground" it is not consistently accurate 
    enough to be used as input to MonthlyTotalPrecipitation.
"""

from sqlalchemy import (
    func,
    and_,
    not_,
    case,
    cast,
    Column,
    Integer,
    String,
    Date,
    DateTime,
    Float,
    Index,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Query

from pycds.context import get_schema_name
from pycds.orm.tables import (
    History,
    Obs,
    ObsRawNativeFlags,
    NativeFlag,
    ObsRawPCICFlags,
    PCICFlag,
    Variable,
)
from pycds.alembic.extensions.replaceable_objects import ReplaceableNativeMatview
from pycds.orm.view_base import make_declarative_base


Base = make_declarative_base()


# Quality control subquery used in all daily temperature extrema and
# monthly total precip matviews
# This query returns all Obs items with no `discard==True` flags attached.
good_obs = (
    Query(
        [
            Obs.id.label("id"),
            Obs.time.label("time"),
            Obs.mod_time.label("mod_time"),
            Obs.datum.label("datum"),
            Obs.vars_id.label("vars_id"),
            Obs.history_id.label("history_id"),
        ]
    )
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
).subquery("good_obs")


# Subqueries and class used to generate MonthlyTotalPrecipitation mayview
def monthly_total_precipitation_with_total_coverage():
    """Return a SQLAlchemy query for monthly total of precipitation,
    with monthly total data coverage.

    Returns:
        sqlalchemy.sql.expression.Query
    """
    return (
        Query(
            [
                History.id.label("history_id"),
                good_obs.c.vars_id.label("vars_id"),
                func.date_trunc("month", good_obs.c.time).label("obs_month"),
                func.sum(good_obs.c.datum).label("statistic"),
                func.sum(
                    case(
                        {"daily": 1.0, "12-hourly": 0.5, "1-hourly": 1 / 24},
                        value=History.freq,
                    )
                ).label("total_data_coverage"),
            ]
        )
        .select_from(good_obs)
        .join(History)
        .join(Variable)
        .filter(
            Variable.standard_name.in_(
                (
                    "lwe_thickness_of_precipitation_amount",
                    "thickness_of_rainfall_amount",
                    "thickness_of_snowfall_amount",
                )
            )
        )
        .filter(Variable.cell_method == "time: sum")
        .filter(Variable.name != "cum_pcpn_amt")
        .filter(History.freq.in_(("1-hourly", "daily")))
        .group_by(History.id, good_obs.c.vars_id, "obs_month")
    )


def monthly_total_precipitation_with_avg_coverage():
    """Return a SQLAlchemy query for monthly total precipitation,
    with monthly average data coverage.

    Returns:
        sqlalchemy.sql.expression.Query
    """
    # TODO: Rename. Geez.

    monthly_total_precip = monthly_total_precipitation_with_total_coverage().subquery(
        "monthly_total_precip"
    )

    func_schema = getattr(func, get_schema_name())

    return Query(
        [
            monthly_total_precip.c.history_id.label("history_id"),
            monthly_total_precip.c.vars_id.label("vars_id"),
            monthly_total_precip.c.obs_month.label("obs_month"),
            monthly_total_precip.c.statistic.label("statistic"),
            (
                monthly_total_precip.c.total_data_coverage
                / func_schema.DaysInMonth(cast(monthly_total_precip.c.obs_month, Date))
            ).label("data_coverage"),
        ]
    ).select_from(monthly_total_precip)


class MonthlyTotalPrecipitation(Base, ReplaceableNativeMatview):
    __tablename__ = "monthly_total_precipitation_mv"

    history_id = Column(Integer, primary_key=True)
    vars_id = Column(Integer, primary_key=True)
    obs_month = Column(DateTime, primary_key=True)
    statistic = Column(Float)
    data_coverage = Column(Float)

    __selectable__ = monthly_total_precipitation_with_avg_coverage().selectable


# Subquery and classes used to construct the daily temperature extreme matviews
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
        Query(
            [
                History.id.label("history_id"),
                good_obs.c.vars_id.label("vars_id"),
                func_schema.effective_day(
                    good_obs.c.time,
                    cast(extremum, String),
                    cast(History.freq, String),
                ).label("obs_day"),
                extremum_func(good_obs.c.datum).label("statistic"),
                func.sum(
                    case(
                        {"daily": 1.0, "12-hourly": 0.5, "1-hourly": 1 / 24},
                        value=History.freq,
                    )
                ).label("data_coverage"),
            ]
        )
        .select_from(good_obs)
        .join(Variable)
        .join(History)
        .filter(Variable.standard_name == "air_temperature")
        .filter(
            Variable.cell_method.in_(
                (f"time: {extremum}imum", "time: point", "time: mean")
            )
        )
        .filter(History.freq.in_(("1-hourly", "12-hourly", "daily")))
        .group_by(History.id, good_obs.c.vars_id, "obs_day")
    )


class DailyMaxTemperature(Base, ReplaceableNativeMatview):
    __tablename__ = "daily_max_temperature_mv"

    history_id = Column(Integer, primary_key=True)
    vars_id = Column(Integer, primary_key=True)
    obs_day = Column(DateTime, primary_key=True)
    statistic = Column(Float)
    data_coverage = Column(Float)

    __selectable__ = daily_temperature_extremum("max").selectable


class DailyMinTemperature(Base, ReplaceableNativeMatview):
    __tablename__ = "daily_min_temperature_mv"

    history_id = Column(Integer, primary_key=True)
    vars_id = Column(Integer, primary_key=True)
    obs_day = Column(DateTime, primary_key=True)
    statistic = Column(Float)
    data_coverage = Column(Float)

    __selectable__ = daily_temperature_extremum("min").selectable


# subqueries and classes used to construct monthly temperature extreme matviews
def monthly_average_of_daily_temperature_extremum_with_total_coverage(
    extremum,
):
    """Return a SQLAlchemy query for a monthly average of a specified
    extremum of daily temperature, with monthly total data coverage.

    Args:
        extremum (str): 'max' | 'min'

    Returns:
        sqlalchemy.sql.expression.Query
    """
    # TODO: Rename. Geez.

    DailyExtremeTemp = DailyMaxTemperature if extremum == "max" else DailyMinTemperature

    obs_month = func.date_trunc("month", DailyExtremeTemp.obs_day)

    return (
        Query(
            [
                DailyExtremeTemp.history_id.label("history_id"),
                DailyExtremeTemp.vars_id.label("vars_id"),
                obs_month.label("obs_month"),
                func.avg(DailyExtremeTemp.statistic).label("statistic"),
                func.sum(DailyExtremeTemp.data_coverage).label("total_data_coverage"),
            ]
        )
        .select_from(DailyExtremeTemp)
        .group_by(DailyExtremeTemp.history_id, DailyExtremeTemp.vars_id, "obs_month")
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

    avg_daily_extreme_temperature = (
        monthly_average_of_daily_temperature_extremum_with_total_coverage(
            extremum
        ).subquery("avg_daily_extreme_temperature")
    )

    func_schema = getattr(func, get_schema_name())

    return Query(
        [
            avg_daily_extreme_temperature.c.history_id.label("history_id"),
            avg_daily_extreme_temperature.c.vars_id.label("vars_id"),
            avg_daily_extreme_temperature.c.obs_month.label("obs_month"),
            avg_daily_extreme_temperature.c.statistic.label("statistic"),
            (
                avg_daily_extreme_temperature.c.total_data_coverage
                / func_schema.DaysInMonth(
                    cast(avg_daily_extreme_temperature.c.obs_month, Date)
                )
            ).label("data_coverage"),
        ]
    ).select_from(avg_daily_extreme_temperature)


class MonthlyAverageOfDailyMaxTemperature(Base, ReplaceableNativeMatview):
    __tablename__ = "monthly_average_of_daily_max_temperature_mv"

    history_id = Column(Integer, primary_key=True)
    vars_id = Column(Integer, primary_key=True)
    obs_month = Column(DateTime, primary_key=True)
    statistic = Column(Float)
    data_coverage = Column(Float)

    __selectable__ = monthly_average_of_daily_temperature_extremum_with_avg_coverage(
        "max"
    ).selectable


class MonthlyAverageOfDailyMinTemperature(Base, ReplaceableNativeMatview):
    __tablename__ = "monthly_average_of_daily_min_temperature_mv"

    history_id = Column(Integer, primary_key=True)
    vars_id = Column(Integer, primary_key=True)
    obs_month = Column(DateTime, primary_key=True)
    statistic = Column(Float)
    data_coverage = Column(Float)

    __selectable__ = monthly_average_of_daily_temperature_extremum_with_avg_coverage(
        "min"
    ).selectable


Index(
    "monthly_total_precipitation_mv_idx",
    MonthlyTotalPrecipitation.history_id,
    MonthlyTotalPrecipitation.vars_id,
)

Index(
    "daily_max_temperature_mv_idx",
    DailyMaxTemperature.history_id,
    DailyMaxTemperature.vars_id,
)

Index(
    "daily_min_temperature_mv_idx",
    DailyMinTemperature.history_id,
    DailyMinTemperature.vars_id,
)

Index(
    "monthly_average_of_daily_max_temperature_mv_idx",
    MonthlyAverageOfDailyMaxTemperature.history_id,
    MonthlyAverageOfDailyMaxTemperature.vars_id,
)

Index(
    "monthly_average_of_daily_min_temperature_mv_idx",
    MonthlyAverageOfDailyMinTemperature.history_id,
    MonthlyAverageOfDailyMinTemperature.vars_id,
)
