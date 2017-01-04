"""Weather anomaly views

Define views for weather anomaly applications using tools in pycds.view_helpers.

"""

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import text, column
from sqlalchemy.schema import DDL

from pycds.materialized_view_helpers import MaterializedViewMixin

Base = declarative_base()
metadata = Base.metadata


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
        CREATE OR REPLACE FUNCTION effective_day(
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

# Materialized view: Daily maximum temperature
# Materialized view: Daily minimum temperature
#   - These views support views that deliver monthly average of daily max/min temperature.
#   - Observations flagged with meta_native_flag.discard or meta_pcic_flag.discard are not included in the view.
#   - data_coverage is the fraction of observations actually available in a day relative to those potentially available
#      in a day. The computation is correct for a given day if and only if the observation frequency does not change
#      during that day. If such a change does occur, the data_coverage fraction for the day will be > 1, which is not
#      fatal to distinguishing adequate coverage.
#   - These views are defined with plain-text SQL queries instead of with SQLAlchemy select expressions.
#       The SQL SELECT statements were already written, and the work required to translate them to SQLAlchemy seemed
#       unnecessary. See https://docs.sqlalchemy.org/en/latest/core/tutorial.html#using-textual-sql
#       This decision subject to revision.

# TODO: Factor out common subquery for non-discarded obs_raw_id's (as a view)
# TODO: Factor out common query structure into a defined function (parameterized by min/max function [can this be done?]
# and by cell_method

def daily_temperature_extremum_selectable(extremum):
    '''Return a SQLAlchemy selector for a specified extremum of daily temperature.

    Args:
        extremum (str): 'max' | 'min'

    Returns:
        sqlalchemy.sql.expression.FromClause (in fact, sqlalchemy.sql.expression.TextClause)
    '''

    return text('''
        SELECT
            hx.history_id AS history_id,
            obs.vars_id AS vars_id,
            effective_day(obs.obs_time, '{0}', hx.freq) AS obs_day,
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
            obs.obs_raw_id IN (
                -- Return id of each observation without any associated discard flags;
                -- in other words, exclude observations marked discard, and don't be fooled by
                -- additional flags that don't discard (hence the aggregate BOOL_OR's).
                SELECT obs.obs_raw_id
                FROM
                    obs_raw AS obs
                    LEFT JOIN obs_raw_native_flags  AS ornf ON (obs.obs_raw_id = ornf.obs_raw_id)
                    LEFT JOIN meta_native_flag      AS mnf  ON (ornf.native_flag_id = mnf.native_flag_id)
                    LEFT JOIN obs_raw_pcic_flags    AS orpf ON (obs.obs_raw_id = orpf.obs_raw_id)
                    LEFT JOIN meta_pcic_flag        AS mpf  ON (orpf.pcic_flag_id = mpf.pcic_flag_id)
                GROUP BY obs.obs_raw_id
                HAVING
                    BOOL_OR(COALESCE(mnf.discard, FALSE)) = FALSE
                    AND BOOL_OR(COALESCE(mpf.discard, FALSE)) = FALSE
            )
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
