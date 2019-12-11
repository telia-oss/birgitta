import datetime

import pytest  # noqa 401
from birgitta.schema.fixtures import values as v
from birgitta.schema.fixtures.values import dtvals


def test_daterange():
    today = v.today_datetime()
    inthepast = v.inthepast()
    range_len = dtvals.daterange_len(inthepast, today)
    assert range_len == 59


def test_fieldval():
    dt = v.inthepast()
    assert dtvals.datenum(dt) == 20190101
    assert dtvals.yearmonthnum(dt) == 201901
    assert dtvals.yearweekstr(dt) == '01W00'
    assert not dtvals.is_last_day_in_week(dt)
    assert dtvals.yesterdaystr(dt) == '2018-12-31'
    last_month_dt = datetime.datetime(2018, 12, 2, 1, 0,
                                      tzinfo=datetime.timezone.utc)
    assert dtvals.lastmonth(dt) == last_month_dt
