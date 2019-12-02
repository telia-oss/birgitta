from birgitta.schema.fixtures import Fixture, RowConf
from birgitta.schema.fixtures.variants import RowConfsVariant

from ...datasets.contracts import dataset as contracts
from .....shared.schema.fixtures.values import chronicle as cv


fixture = Fixture(contracts)
default_row_confs = [
    RowConf().set_field('customerid', cv.customer_id())
    .set_field('group_account_id', cv.groupid())
    .set_field('priceplan_code', cv.priceplan_code())
]
fixture.set_default_variant(RowConfsVariant(default_row_confs))
# No rows expected on brand_code=44
fixture.add_variant('brand_code_44', RowConfsVariant([]))
