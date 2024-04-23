from sqlalchemy import Index, Column, Integer, Float, DateTime, func
from sqlalchemy.orm import Query
from pycds.alembic.extensions.replaceable_objects import ReplaceableNativeMatview
from pycds.orm.tables import Obs
from pycds.orm.view_base import make_declarative_base

# This file creates six material views. Only one of them is used by
# the backend; the other five are just used to construct it.
#
# The "final" materialized view is CumulativePrecipPetMonth, or
# cum_precip_month_mv. This is the only matview from this file that is
# exported to the ORM-at-large.
#
# The views used to construct it are:
# 1. CumulativePrecipMonthMinMax / cum_pcpn_month_minmax_mv
# 2. CumulativePrecipMonthStartTimesstamps / cum_pcpn_month_start_time_mv
# 3. CumulativePrecipMonthStart / cum_pcpn_month_start_mv 
# 4. CumulativePrecipMonthEndTimesstamps / cum_pcpn_month_end_time_mv
# 5. CumulativePrecipMonthEnd / cum_pcpn_month_end_mv
# 
# CumulativePrecipMonth[Start|End]Timestamps records the first or last timestamp
# for each month + history tuple, and CumulativePrecipMonth[Start|End]
# consumes that to produce a table of the first or last times plus their data. 
# Those tables, along with the minimum and maximum values per month from 
# CumulativePrecipMonthMinMax are consumed by CumulativePrecipPerMonth
# to create a table that, for each history + month records the start and
# end dates of the month, and the earliest, latest, minimum, and maximum
# values recorded that month. 
#
# These values are needed to calculate the monthly precipitation for
# startions that measure precipitation accumulatively - that is, the number
# they report is how much precipitation there has been since the last time
# the measurement was arbitrarily "reset".
#
# So far only stations that record the "cum_pcpn_amt" variable are treated 
# this way. You will need ot produce an updated version of these materialized
# views if other stations are discovered to work this way.

Base = make_declarative_base()


class CumulativePrecipMonthMinmax(Base, ReplaceableNativeMatview):
    """This materialized view is an intermediate step to constructing
    CumulativePrecipPerMonth. It provides the minimum and maximum values
    observed each month for which a station history records the
    cum_pcpn_amt variable.
    
    Definition of supporting view:
    SELECT vars_id, history_id, DATE_TRUNC('month', obs_time) AS "month", max(datum) as "max_datum", min(datum) as "min_datum"
    FROM obs_raw
    WHERE vars_id = 705
    GROUP BY DATE_TRUNC('month', obs_time), vars_id, history_id;
    """
    __tablename__ = "cum_pcpn_month_minmax_mv"
    
    history_id = Column(Integer, primary_key=True)
    vars_id = Column(Integer, primary_key=True)
    month = Column(DateTime)
    min_datum = Column(Float)
    max_datum = Column(Float)

    __selectable__ = (
        Query(
            [
                Obs.history_id.label("history_id"),
                Obs.vars_id.label("vars_id"),
                func.date_trunc('month', Obs.time).label("month"),
                func.min(Obs.datum).label("min_datum"),
                func.max(Obs.datum).label("max_datum")
            ]
        ).filter(vars_id == 705)
        .group_by(Obs.history_id, Obs.vars_id, func.date_trunc('month', Obs.time))
    ).selectable

class CumulativePrecipMonthStartTimestamps(Base, ReplaceableNativeMatview):
    """This materialized view is an intermediate step to constructing
    CumulativePrecipPerMonth. It provides a listing of the earliest
    observation for each month for which a station history records the
    cum_pcpn_amt variable.
    
    Definition of supporting view:
    SELECT vars_id, history_id, DATE_TRUNC('month', obs_time) AS "month", min(obs_time) as "start_time"
    FROM obs_raw                      
    WHERE vars_id = 705
    GROUP BY DATE_TRUNC('month', obs_time), vars_id, history_id;
    """
    __tablename__ = "cum_pcpn_month_start_times_mv"
    
    history_id = Column(Integer, primary_key=True)
    vars_id = Column(Integer, primary_key=True)
    start_time = Column(DateTime)
    month = Column(DateTime)

    __selectable__ = (
        Query(
            [
                Obs.history_id.label("history_id"),
                Obs.vars_id.label("vars_id"),
                func.date_trunc('month', Obs.time).label("month"),
                func.min(Obs.time).label("start_time"),
            ]
        ).filter(vars_id == 705)
        .group_by(Obs.history_id, Obs.vars_id, func.date_trunc('month', Obs.time))
    ).selectable
    
class CumulativePrecipMonthStart(Base, ReplaceableNativeMatview):
    """This materialized view is an intermediate step to constructing
    CumulativePrecipPerMonth. It provides a listing of the time and value
    of the earliest observation per history and variable per month for all
    stations histories using the cum_pcpn_amt variable
    
    Definition of supporting view:
    SELECT                                       
    cum_pcpn_month_start_times_mv.vars_id,
    cum_pcpn_month_start_times_mv.history_id,
    cum_pcpn_month_start_times_mv.month,
    cum_pcpn_month_start_times_mv.start_time,
    obs_raw.datum
    FROM cum_pcpn_month_start_times_mv, obs_raw
    WHERE cum_pcpn_month_start_times_mv.vars_id = obs_raw.vars_id AND
    cum_pcpn_month_start_times_mv.history_id = obs_raw.history_id AND
    cum_pcpn_month_start_times_mv.start_time = obs_raw.obs_time;
    """
    
    __tablename__ = "cum_pcpn_month_start_mv"
    
    history_id = Column(Integer, primary_key=True)
    vars_id = Column(Integer, primary_key=True)
    month = Column(DateTime)
    start_time = Column(DateTime)
    start_datum = Column(Float)

    __selectable__ = (
        Query(
            [
                CumulativePrecipMonthStartTimestamps.history_id.label("history_id"),
                CumulativePrecipMonthStartTimestamps.vars_id.label("vars_id"),
                CumulativePrecipMonthStartTimestamps.month.label("month"),
                CumulativePrecipMonthStartTimestamps.start_time.label("start_time"),
                Obs.datum.label("start_datum"),
            ]
        ).filter(Obs.vars_id == CumulativePrecipMonthStartTimestamps.vars_id)
        .filter(Obs.history_id == CumulativePrecipMonthStartTimestamps.history_id)
        .filter(Obs.time == CumulativePrecipMonthStartTimestamps.start_time)
    ).selectable
    

class CumulativePrecipMonthEndTimestamps(Base, ReplaceableNativeMatview):
    """This materialized view is an intermediate step to constructing
    CumulativePrecipPerMonth. It provides a listing of the latest
    observation for each month for which a station history records the
    cum_pcpn_amt variable.
    
    Definition of supporting view:
    SELECT vars_id, history_id, DATE_TRUNC('month', obs_time) AS "month", max(obs_time) as "end_time"
    FROM obs_raw                      
    WHERE vars_id = 705
    GROUP BY DATE_TRUNC('month', obs_time), vars_id, history_id;
    """
    __tablename__ = "cum_pcpn_month_end_times_mv"
    
    history_id = Column(Integer, primary_key=True)
    vars_id = Column(Integer, primary_key=True)
    end_time = Column(DateTime)
    month = Column(DateTime)

    __selectable__ = (
        Query(
            [
                Obs.history_id.label("history_id"),
                Obs.vars_id.label("vars_id"),
                func.date_trunc('month', Obs.time).label("month"),
                func.max(Obs.time).label("end_time"),
            ]
        ).filter(vars_id == 705)
        .group_by(Obs.history_id, Obs.vars_id, func.date_trunc('month', Obs.time))
    ).selectable
    
    
    
class CumulativePrecipMonthEnd(Base, ReplaceableNativeMatview):
    """This materialized view is an intermediate step to constructing
    CumulativePrecipPerMonth. It provides a listing of the time and value
    of the last observation per history and variable per month for all
    stations histories using the cum_pcpn_amt variable
    
    Definition of supporting view:
    SELECT                                       
    cum_pcpn_month_end_times_mv.vars_id,
    cum_pcpn_month_end_times_mv.history_id,
    cum_pcpn_month_emd_times_mv.month,
    cum_pcpn_month_end_times_mv.start_time,
    obs_raw.datum
    FROM cum_pcpn_month_end_times_mv, obs_raw
    WHERE cum_pcpn_month_end_times_mv.vars_id = obs_raw.vars_id AND
    cum_pcpn_month_end_times_mv.history_id = obs_raw.history_id AND
    cum_pcpn_month_end_times_mv.start_time = obs_raw.obs_time;
    """
    
    __tablename__ = "cum_pcpn_month_end_mv"
    
    history_id = Column(Integer, primary_key=True)
    vars_id = Column(Integer, primary_key=True)
    month = Column(DateTime)
    end_time = Column(DateTime)
    end_datum = Column(Float)

    __selectable__ = (
        Query(
            [
                CumulativePrecipMonthEndTimestamps.history_id.label("history_id"),
                CumulativePrecipMonthEndTimestamps.vars_id.label("vars_id"),
                CumulativePrecipMonthEndTimestamps.month.label("month"),
                CumulativePrecipMonthEndTimestamps.end_time.label("end_time"),
                Obs.datum.label("end_datum"),
            ]
        ).filter(Obs.vars_id == CumulativePrecipMonthEndTimestamps.vars_id)
        .filter(Obs.history_id == CumulativePrecipMonthEndTimestamps.history_id)
        .filter(Obs.time == CumulativePrecipMonthEndTimestamps.end_time)
    ).selectable


class CumulativePrecipPerMonth(Base, ReplaceableNativeMatview):
    """This materialized view speeds up calculation of monthly 
    data from weather stations that use a "tipping bucket" type
    sensor, which records cumulative precipitation since the last 
    time the bucket tipped.
    
    At present, these stations are identified by their reporting
    of a variable called cum_pcpn_amt. If more stations featuring
    tipping buckets are added to our network, this criteria may need
    to updated for a new version of this matview.
    
    It is constituted from three intermediate matviews,
    CumulativePrecipAtMonthEnd, CumulativePrecipAtMonthStart,
    and CumulativePrecipMonthMinMax.

    Definition of supporting *view*:
    
    SELECT
    cum_pcpn_month_minmax_mv.vars_id,
    cum_pcpn_month_minmax_mv.history_id,
    cum_pcpn_month_minmax_mv.month,
    cum_pcpn_month_minmax_mv.min_datum,
    cum_pcpn_month_minmax_mv.max_datum,
    cum_pcpn_month_start_mv.start_time,
    cum_pcpn_month_start_mv.datum AS “start_datum”,
    cum_pcpn_month_end_mv.end_time,
    cum_pcpn_month_end_mv.datum AS “end_datum”
    FROM cum_pcpn_month_minmax_mv, cum_pcpn_month_start_mv, cum_pcpn_month_end_mv  
    WHERE
    cum_pcpn_month_minmax_mv.history_id = cum_pcpn_month_start_mv.history_id AND
    cum_pcpn_month_minmax_mv.vars_id = cum_pcpn_month_start_mv.vars_id AND
    cum_pcpn_month_minmax_mv.month = cum_pcpn_month_start_mv.month AND
    cum_pcpn_month_minmax_mv.history_id = cum_pcpn_month_end_mv.history_id AND
    cum_pcpn_month_minmax_mv.vars_id = cum_pcpn_month_end_mv.vars_id AND
    cum_pcpn_month_minmax_mv.month = cum_pcpn_month_end_mv.month;    
    """

    __tablename__ = "cum_pcpn_per_month_mv"

    history_id = Column(Integer, primary_key=True)
    vars_id = Column(Integer, primary_key=True)
    min_datum = Column(Float)
    max_datum = Column(Float)
    start_datum = Column(Float)
    end_datum = Column(Float)
    month = Column(DateTime)
    start_time = Column(DateTime)
    end_time = Column(DateTime)

    __selectable__ = (
        Query(
            [
                CumulativePrecipMonthMinmax.history_id,
                CumulativePrecipMonthMinmax.vars_id,
                CumulativePrecipMonthMinmax.month,
                CumulativePrecipMonthMinmax.max_datum,
                CumulativePrecipMonthMinmax.min_datum,
                CumulativePrecipMonthStart.start_time,
                CumulativePrecipMonthStart.start_datum,
                CumulativePrecipMonthEnd.end_time,
                CumulativePrecipMonthEnd.end_datum
            ]
        ).filter(CumulativePrecipMonthMinmax.history_id == CumulativePrecipMonthStart.history_id)
        .filter(CumulativePrecipMonthMinmax.vars_id == CumulativePrecipMonthStart.vars_id)
        .filter(CumulativePrecipMonthMinmax.month == CumulativePrecipMonthStart.month)
        .filter(CumulativePrecipMonthMinmax.history_id == CumulativePrecipMonthEnd.history_id)
        .filter(CumulativePrecipMonthMinmax.vars_id == CumulativePrecipMonthEnd.vars_id)
        .filter(CumulativePrecipMonthMinmax.month == CumulativePrecipMonthEnd.month)
    ).selectable


Index("cum_pcpn_per_month_idx", CumulativePrecipPerMonth.vars_id, CumulativePrecipPerMonth.history_id, CumulativePrecipPerMonth.month)