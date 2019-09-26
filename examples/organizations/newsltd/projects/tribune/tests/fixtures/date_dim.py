import datetime

from birgitta.schema import fixtures
from birgitta.schema.fixtures.values import dtvals

from ...datasets.date_dim import schema


def fx_default(spark):
    rows = []
    for dt in dtvals.daterange():
        rows.append(date_row(dt))

    return fixtures.df_w_rows(spark, schema, rows)


def escape_date_types(val):
    if type(val) == datetime.date:
        return val.strftime('%Y-%m-%d %H:%M:%S')
    if type(val) == datetime.datetime:
        return val.strftime('%Y-%m-%d')
    return val


def date_row(dt):
    return [
        escape_date_types(
            dtvals.field_val(f, dt)
        ) for f in schema.fields()
    ]
