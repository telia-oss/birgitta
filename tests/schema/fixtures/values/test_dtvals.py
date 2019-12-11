import datetime

import holidays
import pytest  # noqa 401
from birgitta.schema.fixtures import values as v
from birgitta.schema.fixtures.values import dtvals


def test_daterange():
    today = v.today_datetime()
    inthepast = v.inthepast()
    range_len = dtvals.daterange_len(inthepast, today)
    assert range_len == 59
    range_len = dtvals.daterange_len(None, today)
    assert range_len == 59
    range_len = dtvals.daterange_len(None, None)
    assert range_len == 59


def test_vals():
    date_val = v.inthepast()
    assert datetime.datetime(2019, 1, 1, 0, 0) == dtvals.timestamp(date_val)
    assert datetime.datetime(2019, 1, 1, 0, 0) == dtvals.timestamp(None)
    assert 20190101 == dtvals.datenum(date_val)
    assert 201901 == dtvals.yearmonthnum(date_val)
    assert '01W00' == dtvals.yearweekstr(date_val)
    assert 9 == dtvals.weeks_ago()
    assert 13 == dtvals.weeks_ago(date=datetime.date(2018, 12, 1))
    assert 5 == dtvals.weeks_ago(
        date=datetime.date(2018, 11, 1),
        today=datetime.date(2018, 12, 1))
    assert not dtvals.is_last_day_in_week(date_val)
    assert 1 == dtvals.day_in_year()
    assert '2019-01-09' == dtvals.second_yearweekstr()
    assert datetime.date(2019, 1, 9) == dtvals.second_yearweek()
    assert 1 == dtvals.isholidayint(datetime.date(2018, 12, 25))
    assert dtvals.yesterdaystr(date_val) == '2018-12-31'
    last_month_date = datetime.datetime(2018, 12, 2, 1, 0,
                                        tzinfo=datetime.timezone.utc)
    assert dtvals.lastmonth(date_val) == last_month_date


def test_isholiday():
    assert dtvals.isholiday()
    assert dtvals.isholiday(None)
    assert dtvals.isholiday(date=None)
    assert not dtvals.isholiday(date=datetime.date(2018, 12, 22))
    assert not dtvals.isholiday(datetime.date(2018, 12, 22))
    assert dtvals.isholiday()
    assert dtvals.isholiday(None, None)
    assert dtvals.isholiday(hdays=None)
    assert dtvals.isholiday(date=datetime.date(2019, 1, 14),
                            hdays=holidays.Japan())
    assert not dtvals.isholiday(date=datetime.date(2019, 1, 15),
                                hdays=holidays.Japan())


def test_utils():
    assert '2019-03-01 00:00:00' == dtvals.date_types_to_str(v.today())
    assert datetime.datetime(2019, 1, 1, 0, 0) == dtvals.date_to_datetime(
        datetime.date(2019, 1, 1))
