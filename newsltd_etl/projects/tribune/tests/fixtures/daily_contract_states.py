from birgitta.schema.fixtures import ExampleVal, Fixture, RowConf
from birgitta.schema.fixtures import values as v
from birgitta.schema.fixtures.values import dtvals
from birgitta.schema.fixtures.variants import RowConfsVariant

from ...datasets.daily_contract_states import dataset as daily_contract_states

row_confs = []
for dt in dtvals.daterange():
    row_confs.append(
        RowConf().set_field('datestamp', dt)
        .set_field('end_date', v.today())
    )

fixture = Fixture(daily_contract_states)
fixture.set_default_variant(RowConfsVariant(row_confs))
