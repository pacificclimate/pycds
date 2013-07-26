from datetime import datetime

from pycds.util import orm_station_table

def test_station_table(test_session):
    table = orm_station_table(test_session, 5534)
    values = [ x for x in table ]

    last_time = datetime(1950, 1, 1)
    # times should be monotonically increasing
    for a_time, min_temp, min_temp_flag, accppt1, accppt1_flag, accppt2, accpp2_flag, snow, snow_flag in values:
        assert type(a_time) == datetime
        assert last_time < a_time
        last_time = a_time

    beginning_tmin = [0.9, 3.8, 1.6, 2.8, 4.0, 13.9, -1.4, 1.2, 1.7, 2.1, 4.4, 5.1, 7.6, 7.3, 8.9, 4.5, 8.4, 6.9, 5.1, 6.2]
    for t, (a_time, min_temp, min_temp_flag, accppt1, accppt1_flag, accppt2, accpp2_flag, snow, snow_flag) in zip(beginning_tmin, values[0:20]):
        assert t == min_temp

    assert values[4][2] == 'MoE_AP_1'
    assert values[4][2] == 'MoE_AP_1'

    last_tmax = [120., 520., 240.]
    for accppt1, x in zip(last_tmax, values[-3:]):
        assert x[4] == accppt2
