from birgitta.schema import fixtures
from ...datasets.contract_data import schema # noqa 501


def fx_default(spark):
    row_confs = [{}]
    return fixtures.df(spark, schema, row_confs)
