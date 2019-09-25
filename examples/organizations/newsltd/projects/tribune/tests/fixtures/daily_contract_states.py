from birgitta.schema import fixtures
from birgitta.schema.fixtures import values as v
from birgitta.schema.fixtures.values import dtvals

from ...datasets.daily_contract_states import schema  # noqa: E501


def fx_default(spark):
    row_confs = []
    for dt in dtvals.daterange():
        row_confs.append({
            "datestamp": {"example": {"static": dt}},
            "end_date": {"example": {"fn": v.today}},
        })
    return fixtures.df(spark, schema, row_confs)
