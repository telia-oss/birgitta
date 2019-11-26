from birgitta.schema import fixtures
from birgitta.schema.fixtures import values as v
from ...datasets.filtered_contracts import schema # noqa 501


def fx_default(spark):
    row_confs = [{'end_date': {"example": {"fn": v.today}}}]
    return fixtures.df(spark, schema, row_confs)


def fx_brand_code_44(spark):
    row_confs = []
    return fixtures.df(spark, schema, row_confs)
