"""
Native matviews for weather-anomaly application, optimized to reuse a
materialized set of discarded observation IDs.

This version retains the established weather-anomaly semantics:
observations with any native or PCIC flag whose ``discard`` field is true are
excluded. The difference is that discard status is computed once in
``discarded_obs_raw_mv`` and consumed through an anti-join by the three
dependent views.
"""

from sqlalchemy import (
    func,
    and_,
    or_,
    not_,
    case,
    cast,
    Column,
    BigInteger,
    Integer,
    String,
    Date,
    DateTime,
    Float,
    Index,
)
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


# Build this from the same relationship-driven joins used by the existing
# good_obs definition. Do not refer directly to association-table columns
# here: historical ORM mapping details are intentionally hidden behind these
# relationships.

discarded_obs_raw = (
    Query(
        [
            Obs.id.label("obs_raw_id"),
        ]
    )
    .select_from(Obs)
    .outerjoin(ObsRawNativeFlags)
    .outerjoin(NativeFlag)
    .outerjoin(ObsRawPCICFlags)
    .outerjoin(PCICFlag)
    .group_by(Obs.id)
    .having(
        or_(
            func.bool_or(func.coalesce(NativeFlag.discard, False)),
            func.bool_or(func.coalesce(PCICFlag.discard, False)),
        )
    )
)


class DiscardedObsRaw(Base, ReplaceableNativeMatview):
    __tablename__ = "discarded_obs_raw_mv"

    # The Python attribute is ``id``; the physical materialized-view column
    # remains ``obs_raw_id`` to match the source identifier.
    id = Column("obs_raw_id", BigInteger, primary_key=True)

    __selectable__ = discarded_obs_raw.selectable


Index(
    "discarded_obs_raw_mv_pkey",
    DiscardedObsRaw.id,
    unique=True,
)


# Shared source for daily temperature extrema and monthly total precipitation.
# Keep observations absent from the discarded-ID materialized view.
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
    .outerjoin(DiscardedObsRaw, Obs.id == DiscardedObsRaw.id)
    .filter(DiscardedObsRaw.id.is_(None))
).subquery("good_obs")


def monthly_total_precipitation_with_total_coverage():
    return (
        Query(
            [
                History.id.label("history_id"),
                good_obs.c.vars_id.label("vars_id"),
                func.date_trunc("month", good_obs.c.time).label("obs_month"),
                func.sum(good_obs.c.datum).label("statistic"),
                func.sum(
                    case(
                        {
                            "daily": 1.0,
                            "12-hourly": 0.5,
                            "1-hourly": 1 / 24,
                        },
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
        .filter(not_(Variable.name == "cum_pcpn_amt"))
        .filter(History.freq.in_(("1-hourly", "daily")))
        .group_by(History.id, good_obs.c.vars_id, "obs_month")
    )


def monthly_total_precipitation_with_avg_coverage():
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


def daily_temperature_extremum(extremum):
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
                        {
                            "daily": 1.0,
                            "12-hourly": 0.5,
                            "1-hourly": 1 / 24,
                        },
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
                (
                    f"time: {extremum}imum",
                    "time: point",
                    "time: mean",
                )
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


def monthly_average_of_daily_temperature_extremum_with_total_coverage(
    extremum,
):
    daily_extreme = DailyMaxTemperature if extremum == "max" else DailyMinTemperature
    obs_month = func.date_trunc("month", daily_extreme.obs_day)

    return (
        Query(
            [
                daily_extreme.history_id.label("history_id"),
                daily_extreme.vars_id.label("vars_id"),
                obs_month.label("obs_month"),
                func.avg(daily_extreme.statistic).label("statistic"),
                func.sum(daily_extreme.data_coverage).label("total_data_coverage"),
            ]
        )
        .select_from(daily_extreme)
        .group_by(daily_extreme.history_id, daily_extreme.vars_id, "obs_month")
    )


def monthly_average_of_daily_temperature_extremum_with_avg_coverage(
    extremum,
):
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
