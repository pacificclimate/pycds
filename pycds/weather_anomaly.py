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

"""

import sqlalchemy
from sqlalchemy import MetaData
from sqlalchemy import select, union
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import DDL
from sqlalchemy.sql import text, column

from pycds import ObsRawNativeFlags, NativeFlag, ObsRawPCICFlags, PCICFlag
from pycds.materialized_view_helpers import MaterializedViewMixin
from pycds.view_helpers import ViewMixin

Base = declarative_base(metadata=MetaData(schema='crmp'))
metadata = Base.metadata


class DiscardedObs(Base, ViewMixin):
    """This class represents a view which returns the id's of all observations that have been discarded,
    either by a native flag or a PCIC flag."""
    __selectable__ = union(
        select([ObsRawNativeFlags.c.obs_raw_id.label('id')])
        .select_from(ObsRawNativeFlags.join(NativeFlag))
        .where(NativeFlag.discard),
        # TODO: the following would be better, but I cannot figure out how to bind a param outside of a session
        # .where(func.coalesce(PCICFlag.discard, False)),

        select([ObsRawPCICFlags.c.obs_raw_id.label('id')])\
        .select_from(ObsRawPCICFlags.join(PCICFlag)) \
        .where(PCICFlag.discard),
        # TODO: the following would be better, but I cannot figure out how to bind a param outside of a session
        # .where(func.coalesce(PCICFlag.discard, False)),
    )
    __primary_key__ = ['id']


# This function returns the day that should be used (the effective day) for computing daily temperature extrema.
# It maps the actual observation day to an effective day that causes it to be aggregated within the appropriate 24-hour
# period.
#
# Effective day depends on the extremum being computed (max, min) and the observation frequency (and, technically,
# the network, but for now it seems that 12-hour frequency is only used in a single network).
#
# For maximum temperature:
#   For 1-hour and daily frequency:
#       the effective day is day of observation
#   For 12-hour frequency:
#       nominal reporting times are 0700 and 1600 local; we use noon (1200) to divide morning from afternoon reports
#       the period over which the max should be taken is from noon the day before to noon of the given day
#       the effective day is advanced by one day for afternoon observations
#
# For minimum temperature:
#   effective day does not depend on observation frequency; it is always the day of observation
sqlalchemy.event.listen(
    metadata, 'before_create',
    DDL('''
        CREATE OR REPLACE FUNCTION crmp.effective_day(
          obs_time timestamp without time zone, extremum varchar, freq varchar = ''
        )
        RETURNS timestamp without time zone AS $$
        DECLARE
          offs INTERVAL; -- 'offset' is a reserved word
          hour INTEGER;
        BEGIN
          offs := '0'::INTERVAL;
          IF extremum = 'max' THEN
              hour := date_part('hour', obs_time);
              IF freq = '12-hourly' AND hour >= 12 THEN
                offs = '1 day'::INTERVAL;
              END IF;
          ELSE
            offs = '0'::INTERVAL;
          END IF;
          RETURN date_trunc('day', obs_time) + offs;
        END;
        $$ LANGUAGE plpgsql;
    ''')
)


def daily_temperature_extremum_selectable(extremum):
    """Return a SQLAlchemy selector for a specified extremum of daily temperature.

    Args:
        extremum (str): 'max' | 'min'

    Returns:
        sqlalchemy.sql.expression.FromClause (in fact, sqlalchemy.sql.expression.TextClause)
    """

    return text('''
        SELECT
            hx.history_id AS history_id,
            obs.vars_id AS vars_id,
            effective_day(obs.obs_time, '{0}', hx.freq::varchar) AS obs_day,
            {0}(obs.datum) AS statistic,
            sum(
                CASE hx.freq
                WHEN 'daily' THEN 1.0::float
                WHEN '1-hourly' THEN (1/24.0)::float
                WHEN '12-hourly' THEN 0.5::float
                END
            ) AS data_coverage
        FROM
            obs_raw AS obs
            INNER JOIN meta_vars AS vars USING (vars_id)
            INNER JOIN meta_history AS hx USING (history_id)
        WHERE
            obs.obs_raw_id NOT IN (SELECT id FROM discarded_obs_v)
            AND vars.standard_name = 'air_temperature'
            AND vars.cell_method IN ('time: {0}imum', 'time: point', 'time: mean')
            AND hx.freq IN ('1-hourly', '12-hourly', 'daily')
        GROUP BY
            hx.history_id, vars_id, obs_day
    '''.format(extremum))\
        .columns(
                column('history_id'),
                column('vars_id'),
                column('obs_day'),
                column('statistic'),
                column('data_coverage')
        )


class DailyMaxTemperature(Base, MaterializedViewMixin):
    __selectable__ = daily_temperature_extremum_selectable('max')
    __primary_key__ = 'history_id vars_id obs_day'.split()


class DailyMinTemperature(Base, MaterializedViewMixin):
    __selectable__ = daily_temperature_extremum_selectable('min')
    __primary_key__ = 'history_id vars_id obs_day'.split()


def monthly_average_of_daily_temperature_extremum_selectable(extremum):
    """Return a SQLAlchemy selector for a monthly average of a specified extremum of daily temperature.

    Args:
        extremum (str): 'max' | 'min'

    Returns:
        sqlalchemy.sql.expression.FromClause (in fact, sqlalchemy.sql.expression.TextClause)
    """

    return text('''
        SELECT
            history_id,
            vars_id,
            obs_month,
            statistic,
            total_data_coverage / DaysInMonth(obs_month::date) as data_coverage
        FROM (
            SELECT
                history_id,
                vars_id,
                date_trunc('month', obs_day) AS obs_month,
                avg(statistic) AS statistic,
                sum(data_coverage) AS total_data_coverage
            FROM
                daily_{0}_temperature_mv
            GROUP BY
                history_id, vars_id, obs_month
        ) AS temp
    '''.format(extremum))\
        .columns(
            column('history_id'),
            column('vars_id'),
            column('obs_month'),
            column('statistic'),
            column('data_coverage')
        )


class MonthlyAverageOfDailyMaxTemperature(Base, MaterializedViewMixin):
    __selectable__ = monthly_average_of_daily_temperature_extremum_selectable('max')
    __primary_key__ = 'history_id vars_id obs_month'.split()


class MonthlyAverageOfDailyMinTemperature(Base, MaterializedViewMixin):
    __selectable__ = monthly_average_of_daily_temperature_extremum_selectable('min')
    __primary_key__ = 'history_id vars_id obs_month'.split()


class MonthlyTotalPrecipitation(Base, MaterializedViewMixin):
    __selectable__ = text('''
        SELECT
            history_id,
            vars_id,
            obs_month,
            statistic,
            total_data_coverage / DaysInMonth(obs_month::date) as data_coverage
        FROM (
            SELECT
                hx.history_id,
                obs.vars_id,
                date_trunc('month', obs_time) AS obs_month,
                sum(datum) AS statistic,
                sum(
                    CASE hx.freq
                    WHEN 'daily' THEN 1.0::float
                    WHEN '1-hourly' THEN (1.0 / 24.0)::float
                    END
                ) AS total_data_coverage
            FROM
                obs_raw AS obs
                INNER JOIN meta_vars AS vars USING (vars_id)
                INNER JOIN meta_history AS hx USING (history_id)
            WHERE
                obs.obs_raw_id NOT IN (SELECT id FROM discarded_obs_v)
                AND vars.standard_name IN (
                    'lwe_thickness_of_precipitation_amount',
                    'thickness_of_rainfall_amount',
                    'thickness_of_snowfall_amount'
                )
                AND vars.cell_method = 'time: sum'
                AND hx.freq IN ('1-hourly', 'daily')
            GROUP BY
                hx.history_id, vars_id, obs_month
        ) AS temp
    ''').columns(
        column('history_id'),
        column('vars_id'),
        column('obs_month'),
        column('statistic'),
        column('data_coverage')
    )
    __primary_key__ = 'history_id vars_id obs_month'.split()
