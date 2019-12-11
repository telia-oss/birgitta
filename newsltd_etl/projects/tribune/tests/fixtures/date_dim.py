from birgitta.schema.fixtures import Fixture
from birgitta.schema.fixtures.values import dtvals
from birgitta.schema.fixtures.variants import RowsVariant

from ...datasets.date_dim import dataset as date_dim


FIELD_MAP = {
    'date': 'datenum',
    'year4': 'yearnum',
    'is_leap_year': 'is_leap_year',
    'halfyear_number': 'halfyearnum',
    'tertial_number': 'tertialnum',
    'year_quarter': 'yearquarterstr',
    'quarter_number': 'quarternum',
    'year_month_number': 'yearmonthstr',
    'month_number': 'monthnum',
    'isoyear4': 'isoyear4',
    'isoweek': 'isoweek',
    'week_in_year': 'week_in_year',
    'datetimestamp_parsed': 'timestamp',
    'weeks_ago': 'weeks_ago',
    'months_ago': 'months_ago',
    'years_ago': 'years_ago',
    'dates_ago': 'days_ago',
    'date_iso_short': 'dateisoshort',
    'day_in_week': 'day_in_week',
    'day_in_month': 'day_in_month',
    'day_in_year': 'day_in_month',
    'holidayint': 'isholidayint',
    'is_first_day_in_month': 'is_first_day_in_month',
    'is_last_day_in_month': 'is_last_day_in_month'
}


def default_rows_variant():
    rows = []
    for dt in dtvals.daterange():
        rows.append(date_row(dt))
    return RowsVariant(rows)


def date_row(dt):
    return [
        dtvals.date_types_to_str(  # escape to avoid type errors
            getattr(dtvals, FIELD_MAP[f])(dt)
        ) for f in date_dim.schema.fields()
    ]


fixture = Fixture(date_dim)
fixture.set_default_variant(default_rows_variant())
