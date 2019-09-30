"""Various functions for generate date time values.
"""
import calendar
import datetime
import math
from datetime import timezone

import holidays
from birgitta.schema.fixtures import values as v
from dateutil import rrule


no_holidays = holidays.Norway()


def default_val(field):
    """Get the default value for date time named field.

    Args:
        field (str): field name, e.g. 'year4'

    Returns:
        Corresponding value.
    """
    return field_val(field, v.default_date(), v.today())


def field_val(field, dt, today=None):
    if not today:
        today = v.today()

    if field == 'date':
        return datenum(dt)
    elif field == 'period':
        return datenum(dt)
    elif field == 'month':
        return monthnum(dt)
    elif field == 'week':
        return yearweekstr(dt)
    elif field == 'year4':
        return dt.year
    elif field == 'is_leap_year':
        return calendar.isleap(dt.year)
    elif field == 'halfyear_number':
        return roundup_divide_month(dt, 6)
    elif field == 'tertial_number':
        return roundup_divide_month(dt, 4)
    elif field == 'year_quarter':
        return "%s-Q%d" % (dt.strftime("%Y"), quarter(dt))
    elif field == 'quarter_number':
        return quarter(dt)
    elif field == 'year_month_number':
        return dt.strftime("%Y-%m")
    elif field == 'month_number':
        return dt.month
    elif field == 'isoyear4':
        return dt.isocalendar()[0]
    elif field == 'isoweek':
        return dt.isocalendar()[1]
    elif field == 'week_in_year':
        return dt.isocalendar()[1]
    elif field == 'datetimestamp_parsed':
        return date_to_datetime(dt)
    elif field == 'weeks_ago':
        return weeks_between(dt, today)
    elif field == 'months_ago':
        return months_between(dt, today)
    elif field == 'years_ago':
        return years_between(dt, today)
    elif field == 'dates_ago':
        return days_between(dt, today)
    elif field == 'date_iso_short':
        return datenum(dt)
    elif field == 'day_in_week':
        return dt.isocalendar()[2]
    elif field == 'day_in_month':
        return dt.day
    elif field == 'day_in_year':
        return dt.timetuple().tm_yday
    elif field == 'holidayint':
        return is_holiday(dt)
    elif field == 'is_first_day_in_month':
        return dt.day == 1
    elif field == 'is_last_day_in_week':
        return dt.isocalendar()[2] == 7
    elif field == 'is_last_day_in_month':
        return is_last_day_of_month(dt)
    elif field == 'yesterday':
        return yesterday_str(dt)
    elif field == 'today_str':
        return today_str(dt)
    elif field == 'second_yearweek_str':
        return second_yearweek_str(dt)
    elif field == 'yesterday_str':
        return yesterday_str(dt)
    elif field == 'lastmonth':
        return lastmonth(dt)
    return None


def daterange(start_date=None, end_date=None):
    if not start_date:
        start_date = v.default_date()
    if not end_date:
        end_date = v.today()
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def daterange_len(start_date=None, end_date=None):
    if not start_date:
        start_date = v.default_date()
    if not end_date:
        end_date = v.today()
    return int((end_date - start_date).days)


def roundup_divide_month(dt, div):
    return int(math.ceil(dt.month/div))


def date_to_datetime(dt):
    return datetime.datetime(dt.year, dt.month, dt.day)


def quarter(dt):
    return roundup_divide_month(dt, 4)


def is_holiday(dt):
    return 1 if dt in no_holidays else 0


def datenum(dt):
    return int(dt.strftime("%Y%m%d"))


def monthnum(dt):
    return int(dt.strftime("%Y%m"))


def yearweekstr(dt):
    return dt.strftime("%dW%U")


def strtodate(dt, date_str):
    return dt.strftime(datetime.datetime.strptime(date_str, '%Y-%m-%d'))


def is_last_day_of_month(dt):
    # calendar.monthrange return a tuple (weekday of first day of
    # the  month, number of days in month)
    last_day_of_month = calendar.monthrange(dt.year, dt.month)[1]
    # here i check if date is last day of month
    return dt == datetime.date(dt.year, dt.month, last_day_of_month)


def days_between(start_date, end_date):
    days = rrule.rrule(rrule.DAILY, dtstart=start_date, until=end_date)
    return days.count()


def weeks_between(start_date, end_date):
    weeks = rrule.rrule(rrule.WEEKLY, dtstart=start_date, until=end_date)
    return weeks.count()


def months_between(start_date, end_date):
    months = rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date)
    return months.count()


def years_between(start_date, end_date):
    years = rrule.rrule(rrule.YEARLY, dtstart=start_date, until=end_date)
    return years.count()


def yesterday(dt):
    return dt - datetime.timedelta(1)


def dtstr(dt):
    return dt.strftime('%Y-%m-%d')


def today_str(dt):
    return dtstr(dt)


def second_yearweek(dt):
    return dt + datetime.timedelta(8)


def second_yearweek_str(dt):
    return dtstr(second_yearweek(dt))


def yesterday_str(dt):
    return dtstr(yesterday(dt))


def lastmonth(dt):
    return dt - datetime.timedelta(30)


def format_dt(dt):
    if type(dt) is datetime.datetime:
        pydt = dt
    else:
        pydt = dt.to_pydatetime()
    return set_timezone(pydt)


def set_timezone(pydt):
    return pydt.replace(tzinfo=timezone.utc)


def date_types_to_str(val):
    """Convert data types to string. Used to avoid conversion bugs."""
    if type(val) == datetime.date:
        return val.strftime('%Y-%m-%d %H:%M:%S')
    if type(val) == datetime.datetime:
        return val.strftime('%Y-%m-%d')
    return val
