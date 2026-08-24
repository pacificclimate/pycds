"""Opt-in timing checks against a live production database.

Run with, for example::

    PYCDS_PRODUCTION_DATABASE_URL=postgresql://... \
    pytest -m production_performance --log-cli-level=INFO \
        tests/behavioural/functions/test_station_query_production_performance.py

The test is skipped unless ``PYCDS_PRODUCTION_DATABASE_URL`` is set. Its
transaction is read-only and is rolled back after all station queries. By
default it discovers single-history and multi-history stations at the 99th,
95th, 80th, 50th, 20th, 5th, and 1st observation-count percentiles. Set
``PYCDS_PERFORMANCE_STATION_IDS`` to bypass discovery for a targeted run.
"""

import logging
import os
from time import perf_counter

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from pycds.orm.station_queries import query_one_station


logger = logging.getLogger(__name__)

DATABASE_URL_ENV = "PYCDS_PRODUCTION_DATABASE_URL"
STATION_IDS_ENV = "PYCDS_PERFORMANCE_STATION_IDS"
PERCENTILES = (99, 95, 80, 50, 20, 5, 1)

DISCOVER_STATIONS = text(
    """
    WITH station_counts AS (
        SELECT
            station_id,
            count(*)::integer AS history_count,
            sum(obs_count)::bigint AS observation_count
        FROM crmp.station_obs_stats_mv
        GROUP BY station_id
    ),
    candidate_stations AS (
        SELECT
            CASE
                WHEN history_count = 1 THEN 'single_history'
                ELSE 'multiple_histories'
            END AS cohort,
            station_id,
            history_count,
            observation_count
        FROM station_counts
    ),
    requested_percentiles(percentile, position) AS (
        VALUES
            (0.99::double precision, 1),
            (0.95, 2),
            (0.80, 3),
            (0.50, 4),
            (0.20, 5),
            (0.05, 6),
            (0.01, 7)
    ),
    percentile_counts AS (
        SELECT
            stations.cohort,
            percentiles.percentile,
            percentiles.position,
            percentile_disc(percentiles.percentile)
                WITHIN GROUP (ORDER BY stations.observation_count)
                AS observation_count
        FROM candidate_stations AS stations
        CROSS JOIN requested_percentiles AS percentiles
        GROUP BY
            stations.cohort,
            percentiles.percentile,
            percentiles.position
    )
    SELECT
        targets.cohort,
        round((targets.percentile * 100)::numeric)::integer AS percentile,
        min(stations.station_id) AS station_id,
        min(stations.history_count) AS history_count,
        targets.observation_count
    FROM percentile_counts AS targets
    JOIN candidate_stations AS stations
        ON stations.cohort = targets.cohort
       AND stations.observation_count = targets.observation_count
    GROUP BY
        targets.cohort,
        targets.percentile,
        targets.position,
        targets.observation_count
    ORDER BY targets.cohort, targets.position
    """
)


def _explicit_station_ids() -> tuple[int, ...] | None:
    value = os.environ.get(STATION_IDS_ENV)
    if value is None:
        return None

    try:
        station_ids = tuple(int(item.strip()) for item in value.split(","))
    except ValueError as error:
        raise pytest.UsageError(
            f"{STATION_IDS_ENV} must be a comma-separated list of integers"
        ) from error

    if len(station_ids) < 2 or any(station_id <= 0 for station_id in station_ids):
        raise pytest.UsageError(
            f"{STATION_IDS_ENV} must contain at least two positive station IDs"
        )
    return station_ids


def _stations(session: Session) -> list[dict]:
    explicit_ids = _explicit_station_ids()
    if explicit_ids is not None:
        return [
            {
                "cohort": "explicit",
                "percentile": None,
                "station_id": station_id,
                "history_count": None,
                "observation_count": None,
            }
            for station_id in explicit_ids
        ]

    stations = [dict(row) for row in session.execute(DISCOVER_STATIONS).mappings()]
    assert len(stations) == 2 * len(PERCENTILES)
    return stations


def _format_results(results: list[dict]) -> str:
    headers = (
        "Cohort",
        "Percentile",
        "Station",
        "Histories",
        "Estimated obs",
        "Rows",
        "Build",
        "First row",
        "Remaining",
        "Total",
    )
    rows = [
        (
            result["cohort"],
            f"p{result['percentile']}" if result["percentile"] else "-",
            str(result["station_id"]),
            str(result["history_count"] or "-"),
            (
                f"{result['observation_count']:,}"
                if result["observation_count"] is not None
                else "-"
            ),
            f"{result['row_count']:,}",
            f"{result['build_seconds']:.3f}s",
            f"{result['first_row_seconds']:.3f}s",
            f"{result['remaining_rows_seconds']:.3f}s",
            f"{result['total_seconds']:.3f}s",
        )
        for result in results
    ]
    widths = [
        max(len(header), *(len(row[index]) for row in rows))
        for index, header in enumerate(headers)
    ]

    def format_row(row):
        return (
            "| "
            + " | ".join(value.ljust(widths[index]) for index, value in enumerate(row))
            + " |"
        )

    separator = tuple("-" * width for width in widths)
    return "\n".join(
        [format_row(headers), format_row(separator), *(format_row(row) for row in rows)]
    )


@pytest.mark.production_performance
@pytest.mark.skipif(
    DATABASE_URL_ENV not in os.environ,
    reason=f"set {DATABASE_URL_ENV} to run against a live database",
)
def test_query_one_station_production_timings():
    """Discover representative production stations and log query timings."""
    engine = create_engine(
        os.environ[DATABASE_URL_ENV],
        poolclass=NullPool,
    )

    try:
        with engine.connect() as connection, connection.begin():
            connection.execute(text("SET TRANSACTION READ ONLY"))
            session = Session(bind=connection)
            timing_results = []

            for station in _stations(session):
                station_id = station["station_id"]
                started = perf_counter()
                statement = query_one_station(session, station_id)
                query_built = perf_counter()

                result = session.execute(
                    statement.execution_options(
                        stream_results=True,
                        yield_per=10_000,
                    )
                )
                first_row = next(result, None)
                first_row_received = perf_counter()
                row_count = int(first_row is not None) + sum(1 for _ in result)
                finished = perf_counter()

                timing_results.append(
                    {
                        **station,
                        "row_count": row_count,
                        "build_seconds": query_built - started,
                        "first_row_seconds": first_row_received - query_built,
                        "remaining_rows_seconds": finished - first_row_received,
                        "total_seconds": finished - started,
                    }
                )

            logger.info(
                "query_one_station production timings:\n%s",
                _format_results(timing_results),
            )
            session.close()
            connection.rollback()
    finally:
        engine.dispose()
