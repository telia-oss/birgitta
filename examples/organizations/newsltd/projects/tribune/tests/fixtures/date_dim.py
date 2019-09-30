from birgitta.schema import fixtures
from birgitta.schema.fixtures.values import dtvals

from ...datasets.date_dim import schema


def fx_default(spark):
    rows = []
    for dt in dtvals.daterange():
        rows.append(date_row(dt))

    return fixtures.df_w_rows(spark, schema, rows)


def date_row(dt):
    return [
        dtvals.date_types_to_str(  # escape to avoid type errors
            dtvals.field_val(f, dt)
        ) for f in schema.fields()
    ]
