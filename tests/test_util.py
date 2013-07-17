from datetime import datetime

from pycds.util import orm_station_table

def test_station_table(test_session):
    table = orm_station_table(test_session, 913)
    values = [ x for x in table ]

    last_time = datetime(1950, 1, 1)
    # times should be monotonically increasing
    for a_time, min_temp, min_temp_flag, max_temp, max_temp_flag, pcp, pcp_flag in values:
        assert type(a_time) == datetime
        assert last_time < a_time
        last_time = a_time

    beginning_precip = [0, 0, 0, 3, 0, 1.8, 5.6, 9.9, 4.8, 0]
    for p, (a_time, min_temp, min_temp_flag, max_temp, max_temp_flag, pcp, pcp_flag) in zip(beginning_precip, values[0:10]):
        assert p == pcp

    assert values[95][6] == 'EC_trace'
    assert values[98][6] == 'EC_trace'

    last_tmax = [13.6, 13.8, 11.6, 13.1, 14.7, 12.2, 11.0, 12.4, 13.6, 12.3]
    for tmax, x in zip(last_tmax, values[-10:]):
        assert x[3] == tmax
