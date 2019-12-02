from birgitta.schema.fixtures import ExampleVal, Fixture, RowConf
from birgitta.schema.fixtures import values as v
from birgitta.schema.fixtures.variants import RowConfsVariant

from ...datasets.filtered_contracts import dataset as filtered_contracts


fixture = Fixture(filtered_contracts)
default_row_confs = [
    RowConf().set_field('end_date', v.today())
]
fixture.set_default_variant(RowConfsVariant(default_row_confs))
fixture.add_variant('brand_code_44', RowConfsVariant([]))
