"""Default fixture values for various type of values.
"""
import pandas as pd
from birgitta.schema.fixtures.values import dtvals


def inthepast():
    """We have a hardcoded default timestamp to get consistent
    test execution"""
    return dtvals.format_date(pd.Timestamp(2019, 1, 1, 1))


def inthepaststr():
    return dtvals.datestr(inthepast())


def default_timestamp():
    return inthepast()


def default_date():
    return default_timestamp().date()


def today():
    """Today is two months after default timestamp.
    Very useful to get consistent test results."""
    return pd.Timestamp(2019, 3, 1, 1).date()


def todaystr():
    return dtvals.todaystr(today())


def yearend():
    return pd.Timestamp(2019, 12, 31, 1).date()


def today_datetime():
    """Today is two months after default timestamp"""
    return dtvals.format_date(pd.Timestamp(2019, 3, 1, 1))


def timestamp(day_delta=0, hour_delta=0, hard_val=None):
    if hard_val:
        if hard_val == "today":
            dt = today()
        else:
            return hard_val
    else:
        dt = default_timestamp() + pd.Timedelta(days=day_delta,
                                                hours=hour_delta)
        dt = dtvals.format_date(dt)
    return dt


def timestampstr():
    return dtvals.datestr(timestamp())


def NaT():
    return None


def nan():
    return None
