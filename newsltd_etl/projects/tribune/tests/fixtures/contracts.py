from birgitta.schema.fixtures import Fixture, RowConf
from birgitta.schema.fixtures.variants import RowConfsVariant

from ...datasets.contracts import dataset as contracts


fixture = Fixture(contracts)
fixture.add_variant('brand_code_44',
                    RowConfsVariant([RowConf().set_field('brand_code', 44)]))
