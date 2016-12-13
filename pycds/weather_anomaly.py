import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import text, column
from sqlalchemy.schema import DDL

from pycds.view_helpers import ViewMixin
from pycds.materialized_view_helpers import MaterializedViewMixin

Base = declarative_base()
metadata = Base.metadata

# "Proper" views - defined using view functionality within SQLAlchemy using tools in pycds.view_helpers
# TODO: Apply materialized views to all weather anomaly views
# To convert a pure view to a materialized view, inherit from MaterializedViewMixin instead.

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

# This defined function returns the day that should be used (the effective day) for computing daily Tmax.
# The effective day depends on the observation frequency (and, technically, the network, but for now it seems
# that 12-hour frequency is only for a single network). Nominal reporting times are 0700 and 1600 local; it is therefore
# sufficient to use noon (1200) to divide morning from afternoon reports. Hence:
#   For 12-hour frequency: for any given day, the period over which the max should be taken is from noon the day
#       before to noon of the given day.
#   For 1-hour and daily frequency: for any given day, the period is midnight to midnight of that day.
sqlalchemy.event.listen(
    metadata, 'before_create',
    DDL('''
        CREATE OR REPLACE FUNCTION effective_day_for_Tmax(obs_time timestamp without time zone, freq varchar)
        RETURNS timestamp without time zone AS $$
        DECLARE
          offs INTERVAL; -- 'offset' is a reserved word
          hour INTEGER;
        BEGIN
          offs := '0'::INTERVAL;
          hour := date_part('hour', obs_time);
          IF freq = '12-hourly' AND hour >= 12 THEN
            offs = '1 day'::INTERVAL;
          END IF;
          RETURN date_trunc('day', obs_time) + offs;
        END;
        $$ LANGUAGE plpgsql;
    ''')
)

class DailyMaxTemperature(Base, ViewMixin):
    __selectable__ = text('''
        SELECT
            hx.history_id AS history_id,
            obs.vars_id AS vars_id,
            effective_day_for_Tmax(obs.obs_time, hx.freq) AS obs_day,
            max(obs.datum) AS statistic,
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
            AND vars.cell_method IN ('time: maximum', 'time: point', 'time: mean')
            AND hx.freq IN ('1-hourly', '12-hourly', 'daily')
        GROUP BY
            hx.history_id, vars_id, obs_day
    ''').columns(
        column('history_id'),
        column('vars_id'),
        column('obs_day'),
        column('statistic'),
        column('data_coverage')
    )
    __primary_key__ = 'history_id vars_id obs_day'.split()

class DailyMinTemperature(Base, ViewMixin):
    __selectable__ = text('''
        SELECT
            hx.history_id AS history_id,
            obs.vars_id AS vars_id,
            date_trunc('day', obs.obs_time) AS obs_day,
            min(obs.datum) AS statistic,
            sum(
                CASE hx.freq
                WHEN 'daily' THEN 1.0::float
                WHEN '1-hourly' THEN (1.0 / 24.0)::float
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
            AND vars.cell_method IN ('time: minimum', 'time: point', 'time: mean')
            AND hx.freq IN ('1-hourly', '12-hourly', 'daily')
        GROUP BY
            hx.history_id, vars_id, obs_day
    ''').columns(
        column('history_id'),
        column('vars_id'),
        column('obs_day'),
        column('statistic'),
        column('data_coverage')
    )
    __primary_key__ = 'history_id vars_id obs_day'.split()

# Materialized View: Monthly average of daily maximum temperature
# Materialized View: Monthly average of daily minimum temperature
#   - data_coverage is the fraction of of observations actually available in a month relative to those potentially available
#       in a month, and is robust to varying reporting frequencies on different days in the month (but see caveat for
#       daily data coverage above).
#   - These views are defined with plain-text SQL queries instead of with SQLAlchemy select expressions.
#       The SQL SELECT statements were already written, and the work required to translate them to SQLAlchemy seemed
#       excessive and unnecessary. See https://docs.sqlalchemy.org/en/latest/core/tutorial.html#using-textual-sql
#
# TODO: Factor out common query structure into a defined function (parameterized by daily extreme temp view)?

class MonthlyAverageOfDailyMaxTemperature(Base, ViewMixin):
    __selectable__ = text('''
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
                daily_max_temperature_v
            GROUP BY
                history_id, vars_id, obs_month
        ) AS temp
    ''').columns(
        column('history_id'),
        column('vars_id'),
        column('obs_month'),
        column('statistic'),
        column('data_coverage')
    )
    __primary_key__ = 'history_id vars_id obs_month'.split()

class MonthlyAverageOfDailyMinTemperature(Base, ViewMixin):
    __selectable__ = text('''
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
                daily_min_temperature_v
            GROUP BY
                history_id, vars_id, obs_month
        ) AS temp
    ''').columns(
        column('history_id'),
        column('vars_id'),
        column('obs_month'),
        column('statistic'),
        column('data_coverage')
    )
    __primary_key__ = 'history_id vars_id obs_month'.split()


# Materialized View: Monthly total precipitation
#   - Observations flagged with meta_native_flag.discard or meta_pcic_flag.discard are not included in the view.
#   - data_coverage is the fraction of observations actually available in a month relative to those potentially
#      available in a month. This computation is correct if and only if the observation frequency does not change
#      during any one day in the month. It remains approximately correct if such days are rare, and remains valid
#      for the purpose of distinguishing adequate coverage.
#   - This view is defined with plain-text SQL queries instead of with SQLAlchemy select expressions.
#       The SQL SELECT statements were already written, and the work required to translate them to SQLAlchemy seemed
#       excessive and unnecessary. See https://docs.sqlalchemy.org/en/latest/core/tutorial.html#using-textual-sql

# TODO: Factor out common subquery for non-discarded obs_raw_id's (as a view)

class MonthlyTotalPrecipitation(Base, ViewMixin):
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
                AND vars.standard_name IN (
                    'lwe_thickness_of_precipitation_amount',
                    'thickness_of_rainfall_amount',
                    'thickness_of_snowfall_amount'  -- verify that this is rainfall equiv!
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
