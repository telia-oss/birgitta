"""Various functions for generate date time values.
"""
import calendar
import datetime
import math
from datetime import timezone

import holidays
from birgitta.schema.fixtures import values as v
from dateutil import rrule

nor_holidays = holidays.Norway()


# Decorators

def default_date_and_today(func):
    def get_default_date_and_today(date=None, today=None):
        if today is None:
            today = v.today()
        if date is None:
            date = v.default_date()
        return func(date, today)
    return get_default_date_and_today


def default_date(func):
    def get_default_date(date=None):
        if date is None:
            date = v.default_date()
        return func(date)
    return get_default_date


def default_date_and_holiday(func):
    def get_default_date_and_holiday(date=None, hdays=None):
        if date is None:
            date = v.default_date()
        if hdays is None:
            hdays = nor_holidays
        return func(date, hdays)
    return get_default_date_and_holiday


# Value functions

@default_date
def timestamp(date=None):
    return datetime.datetime(date.year, date.month, date.day)


@default_date
def datenum(date=None):
    return int(date.strftime("%Y%m%d"))


@default_date
def yearmonthnum(date=None):
    return int(date.strftime("%Y%m"))


@default_date
def yearmonthstr(date=None):
    return date.strftime("%Y-%m")


@default_date
def yearweekstr(date=None):
    return date.strftime("%dW%U")


@default_date
def yearnum(date=None):
    return date.year


@default_date
def is_leap_year(date=None):
    return calendar.isleap(date.year)


@default_date
def halfyearnum(date=None):
    return roundup_divide_month(date, 6)


@default_date
def tertialnum(date=None):
    return roundup_divide_month(date, 4)


@default_date
def yearquarterstr(date=None):
    return "%s-Q%d" % (date.strftime("%Y"), quarternum(date))


@default_date
def quarternum(date=None):
    return roundup_divide_month(date, 4)


@default_date
def monthnum(date=None):
    return date.month


@default_date
def isoyear4(date=None):
    return date.isocalendar()[0]


@default_date
def isoweek(date=None):
    return date.isocalendar()[1]


@default_date
def week_in_year(date=None):
    return date.isocalendar()[1]


@default_date_and_today
def weeks_ago(date=None, today=None):
    return weeks_between(date, today)


@default_date_and_today
def months_ago(date=None, today=None):
    return months_between(date, today)


@default_date_and_today
def years_ago(date=None, today=None):
    return years_between(date, today)


@default_date_and_today
def days_ago(date=None, today=None):
    return days_between(date, today)


@default_date
def dateisoshort(date=None):
    return datenum(date)


@default_date
def day_in_week(date=None):
    return date.isocalendar()[2]


@default_date
def day_in_month(date=None):
    return date.day


@default_date
def day_in_year(date=None):
    return date.timetuple().tm_yday


@default_date
def is_first_day_in_month(date=None):
    return date.day == 1


@default_date
def is_last_day_in_week(date=None):
    return date.isocalendar()[2] == 7


@default_date
def is_last_day_in_month(date=None):
    return is_last_day_of_month(date)


@default_date
def second_yearweekstr(date=None):
    return datestr(second_yearweek(date))


@default_date
def lastmonth(date=None):
    return date - datetime.timedelta(30)


@default_date_and_holiday
def isholiday(date=None, hdays=None):
    """Return 1 if a holiday. Default calendar is Norway
    from module holidays."""
    return date in hdays


@default_date_and_holiday
def isholidayint(date=None, hdays=None):
    """Return 1 if a holiday. Default calendar is Norway
    from module holidays."""
    return 1 if isholiday(date, hdays) else 0


@default_date
def is_last_day_of_month(date):
    # calendar.monthrange return a tuple (weekday of first day of
    # the  month, number of days in month)
    last_day_of_month = calendar.monthrange(date.year, date.month)[1]
    # here i check if date is last day of month
    return date == datetime.date(date.year, date.month, last_day_of_month)


@default_date
def yesterday(date):
    """Get date string of today. Today is Birgitta's default today value."""
    return date - datetime.timedelta(1)


@default_date
def datestr(date):
    return date.strftime('%Y-%m-%d')


@default_date
def todaystr(date=None):
    """Get date string of today. Today is Birgitta's default today value."""
    return datestr(date)


@default_date
def second_yearweek(date):
    return date + datetime.timedelta(8)


@default_date
def yesterdaystr(date):
    return datestr(yesterday(date))


# Utility functions

def date_types_to_str(val):
    """Convert data types to string. Used to avoid conversion bugs."""
    if type(val) == datetime.date:
        return val.strftime('%Y-%m-%d %H:%M:%S')
    if type(val) == datetime.datetime:
        return val.strftime('%Y-%m-%d')
    return val


def daterange(start_date=None, end_date=None):
    if not start_date:
        start_date = v.default_date()
    if not end_date:
        end_date = v.today()
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def daterange_len(start_date=None, end_date=None):
    if not start_date:
        start_date = v.default_timestamp()
    if not end_date:
        end_date = v.today_datetime()
    return int((end_date - start_date).days)


def roundup_divide_month(date, div):
    return int(math.ceil(date.month/div))


def date_to_datetime(date):
    return datetime.datetime(date.year, date.month, date.day)


def format_date(date):
    if type(date) is datetime.datetime:
        pydt = date
    else:
        pydt = date.to_pydatetime()
    return set_timezone(pydt)


def set_timezone(pydt):
    return pydt.replace(tzinfo=timezone.utc)


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
