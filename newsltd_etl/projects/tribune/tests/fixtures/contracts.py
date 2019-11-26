from birgitta.schema import fixtures

from ...datasets.contracts import schema


def fx_default(spark):
    return fixtures.df(spark, schema)


def fx_brand_code_44(spark):
    row_confs = [
        {"brand_code": {"example": {"static": 44}}}
    ]
    return fixtures.df(spark, schema, row_confs)
