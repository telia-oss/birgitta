from birgitta.schema.schema import Schema

from .....shared.schema.catalog.tribune import catalog


FIELDS = [
    ['date', 'bigint'],
    ['year4', 'bigint'],
    ['is_leap_year', 'string'],
    ['halfyear_number', 'bigint'],
    ['tertial_number', 'bigint'],
    ['year_quarter', 'string'],
    ['quarter_number', 'bigint'],
    ['year_month_number', 'string'],
    ['month_number', 'bigint'],
    ['isoyear4', 'bigint'],
    ['isoweek', 'bigint'],
    ['week_in_year', 'bigint'],
    ['datetimestamp_parsed', 'timestamp'],
    ['weeks_ago', 'bigint'],
    ['months_ago', 'bigint'],
    ['years_ago', 'bigint'],
    ['dates_ago', 'bigint'],
    ['date_iso_short', 'bigint'],
    ['day_in_week', 'bigint'],
    ['day_in_month', 'bigint'],
    ['day_in_year', 'bigint'],
    ['holidayint', 'bigint'],
    ['is_first_day_in_month', 'bool'],
    ['is_last_day_in_month', 'bool']
]

schema = Schema(FIELDS, catalog)
