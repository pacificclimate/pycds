"""
Native matview for  weather anomaly application.

`MonthlyTotalPrecipitation`
  - Observations flagged with `meta_native_flag.discard` or
    `meta_pcic_flag.discard` are not included in the view.
  - `data_coverage` is the fraction of observations actually available in a
    month relative to those potentially available in a month. This computation
    is correct if and only if the observation frequency does not change
    during any one day in the month. It remains approximately correct if such
    days are rare, and remains valid for the purpose of distinguishing adequate
    coverage.

Notes:
  - Schema name: See note on this topic in pycds/__init__.py

  - Session-less queries: All queries defined in this module are session-less;
    that is they are created using the `sqlalchemy.orm.query.Query` object
    instead of via `Session.query()`. Such Query objects have no session
    attached, which is required because the design of the manual materialized
    view implementation delays association of the session to the matview until
    it is explicitly created.
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


# Subquery used in daily temperature extrema and monthly total precip queries
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


Index(
    "monthly_total_precipitation_mv_idx",
    MonthlyTotalPrecipitation.history_id,
    MonthlyTotalPrecipitation.vars_id,
)
