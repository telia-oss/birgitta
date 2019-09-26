"""Default fixture values for various type of values.
"""
import pandas as pd
from birgitta.schema.fixtures.values import dtvals


def inthepast():
    """We have a hardcoded default timestamp to get consistent
    test execution"""
    return dtvals.format_dt(pd.Timestamp(2019, 1, 1, 1))


def inthepast_str():
    return dtvals.dtstr(inthepast())


def default_timestamp():
    return inthepast()


def default_date():
    return default_timestamp().date()


def today():
    """Today is two months after default timestamp.
    Very useful to get consistent test results."""
    return pd.Timestamp(2019, 3, 1, 1).date()


def today_str():
    return dtvals.today_str(today())


def year_end():
    return pd.Timestamp(2019, 12, 31, 1).date()


def today_datetime():
    """Today is two months after default timestamp"""
    return dtvals.format_dt(pd.Timestamp(2019, 3, 1, 1))


def timestamp(day_delta=0, hour_delta=0, hard_val=None):
    if hard_val:
        if hard_val == "today":
            dt = today()
        else:
            return hard_val
    else:
        dt = default_timestamp() + pd.Timedelta(days=day_delta,
                                                hours=hour_delta)
        dt = dtvals.format_dt(dt)
    return dt


def timestamp_str():
    return dtvals.dtstr(timestamp())


def NaT():
    return None


def nan():
    return None
