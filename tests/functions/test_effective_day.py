import datetime
from sqlalchemy.sql import text
from pytest import mark


expected_day = {
    'max': {
        '1-hourly': {'2000-01-01 07:23': datetime.datetime(2000, 1, 1), '2000-01-01 16:18': datetime.datetime(2000, 1, 1)},
        '12-hourly': {'2000-01-01 07:23': datetime.datetime(2000, 1, 1), '2000-01-01 16:18': datetime.datetime(2000, 1, 2)},
    },
    'min': {
        '1-hourly': {'2000-01-01 07:23': datetime.datetime(2000, 1, 1), '2000-01-01 16:18': datetime.datetime(2000, 1, 1)},
        '12-hourly': {'2000-01-01 07:23': datetime.datetime(2000, 1, 1), '2000-01-01 16:18': datetime.datetime(2000, 1, 1)},
    }
}

@mark.parametrize('obs_time', ['2000-01-01 07:23', '2000-01-01 16:18'])  # morning and afternoon
@mark.parametrize('freq', ['1-hourly', '12-hourly'])
@mark.parametrize('extremum', ['max', 'min'])
def test_day_of_observation(pycds_sesh, obs_time, extremum, freq):
    # mod_empty_database_session.execute('SET search_path TO crmp')
    result = pycds_sesh.execute(
        text('SELECT effective_day(:obs_time, :extremum, :freq) AS eday'),
        {'obs_time': obs_time, 'extremum': extremum, 'freq': freq}
    ).fetchall()
    assert len(result) == 1
    assert result[0]['eday'] == expected_day[extremum][freq][obs_time]


