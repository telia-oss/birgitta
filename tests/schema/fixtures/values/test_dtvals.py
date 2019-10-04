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
    assert dtvals.field_val('period', dt) == 20190101
    assert dtvals.field_val('month', dt) == 201901
    assert dtvals.field_val('week', dt) == '01W00'
    assert not dtvals.field_val('is_last_day_in_week', dt)
    assert dtvals.field_val('yesterday', dt) == '2018-12-31'
    assert dtvals.field_val('yesterday_str', dt) == '2018-12-31'
    last_month_dt = datetime.datetime(2018, 12, 2, 1, 0,
                                      tzinfo=datetime.timezone.utc)
    assert dtvals.field_val('lastmonth', dt) == last_month_dt
