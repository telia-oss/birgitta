from birgitta.schema import fixtures
from ...datasets.contracts import schema # noqa 501
from .....shared.schema.fixtures.values import chronicle as cv


def fx_default(spark):
    row_confs = [{
        'customerid': {"example": {"fn": cv.customer_id}},
        'group_account_id': {"example": {"fn": cv.groupid}},
        'priceplan_code': {"example": {"fn": cv.priceplan_code}}
    }]
    return fixtures.df(spark, schema, row_confs)
