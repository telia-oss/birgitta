import datetime

import pytest  # noqa 401
from birgitta.schema.fixtures import values as v


def test_vals():
    assert '2019-01-01' == v.inthepaststr()
    assert '2019-03-01' == v.todaystr()
    assert '2019-01-01' == v.timestampstr()
    assert datetime.date(2019, 12, 31) == v.yearend()
    ts = datetime.datetime(2019, 1, 1, 1, 0, tzinfo=datetime.timezone.utc)
    assert ts == v.timestamp()
    assert datetime.date(2019, 3, 1) == v.timestamp(hard_val='today')
    assert datetime.date(2019, 3, 2) == v.timestamp(
        hard_val=datetime.date(2019, 3, 2))
    assert not v.NaT()
    assert not v.nan()
