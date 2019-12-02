from birgitta.schema.fixtures import Fixture
from birgitta.schema.fixtures.values import dtvals
from birgitta.schema.fixtures.variants import RowsVariant

from ...datasets.date_dim import dataset as date_dim


def default_rows_variant():
    rows = []
    for dt in dtvals.daterange():
        rows.append(date_row(dt))
    return RowsVariant(rows)


def date_row(dt):
    return [
        dtvals.date_types_to_str(  # escape to avoid type errors
            dtvals.field_val(f, dt)
        ) for f in date_dim.schema.fields()
    ]


fixture = Fixture(date_dim)
fixture.set_default_variant(default_rows_variant())
