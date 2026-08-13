"""ORM queries for pivoted station-observation tables."""

from sqlalchemy import case, false, func, select
from sqlalchemy.orm import Session

from pycds.orm.native_matviews import VarsPerHistory
from pycds.orm.tables import History, Obs, Variable


def _station_variables(
    session: Session,
    station_id: int,
    climo: bool,
) -> list[tuple[int, str]]:
    """Return variables actually observed at a station, ordered by name."""
    is_climatological = Variable.cell_method.op("~")(r"(within|over)")

    statement = (
        select(VarsPerHistory.vars_id, Variable.name)
        .select_from(VarsPerHistory)
        .join(History, History.id == VarsPerHistory.history_id)
        .join(Variable, Variable.id == VarsPerHistory.vars_id)
        .where(
            History.station_id == station_id,
            is_climatological if climo else ~is_climatological,
        )
        .distinct()
        .order_by(Variable.name)
    )

    # The legacy SQL emits unquoted aliases, which PostgreSQL folds to lower
    # case. Preserve that result-column convention.
    return [
        (vars_id, str(variable_name).lower())
        for vars_id, variable_name in session.execute(statement)
    ]


def _station_history_ids(session: Session, station_id: int) -> list[int]:
    """Return every history belonging to a station in stable order."""
    return list(
        session.scalars(
            select(History.id)
            .where(History.station_id == station_id)
            .order_by(History.id)
        )
    )


def query_one_station(
    session: Session,
    station_id: int,
    climo: bool = False,
):
    """
    Build a pivoted observation query for one station.

    The result has ``obs_time`` as its first column, followed by one column per
    variable actually present at the station. Values from overlapping histories
    retain the legacy ``max(datum)`` resolution rule.
    """
    variables = _station_variables(
        session,
        station_id,
        climo=climo,
    )

    history_ids = _station_history_ids(session, station_id)

    # Unlike the legacy function, an empty variable list is a valid request.
    if not variables or not history_ids:
        return select(Obs.time.label("obs_time")).where(false())

    variable_ids = [vars_id for vars_id, _ in variables]

    variable_columns = [
        func.max(
            case(
                (Obs.vars_id == vars_id, Obs.datum),
                else_=None,
            )
        ).label(variable_name)
        for vars_id, variable_name in variables
    ]

    history_filter = (
        Obs.history_id == history_ids[0]
        if len(history_ids) == 1
        else Obs.history_id.in_(history_ids)
    )

    return (
        select(
            Obs.time.label("obs_time"),
            *variable_columns,
        )
        .select_from(Obs)
        .where(
            history_filter,
            Obs.vars_id.in_(variable_ids),
        )
        .group_by(Obs.time)
        .order_by(Obs.time)
    )


def do_query_one_station(
    session: Session,
    station_id: int,
    *,
    climo: bool = False,
):
    """Execute :func:`query_one_station`."""
    return session.execute(
        query_one_station(
            session,
            station_id,
            climo=climo,
        )
    )
