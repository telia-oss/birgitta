from birgitta.fields.catalog import Catalog

from ...schema.fixtures.values import chronicle as cv


catalog = Catalog()
catalog.add_field(
    'groupid',
    example=cv.groupid(),
    description='Chronicle group id'
)
catalog.add_field(
    'accountid',
    example=cv.accountid(),
    description='Chronicle account id'
)
catalog.add_field(
    'chronicle_account_id',
    example=cv.accountid(),
    description='Chronicle account id'
)
catalog.add_field(
    'cellphone',
    example=cv.cellphone(),
    description='Chronicle phone number'
)
catalog.add_field(
    'enddate_yyyymmdd',
    example=cv.enddate_yyyymmdd(),
    description='Chronicle contract end date.'
)
catalog.add_field(
    'startdate_yyyymmdd',
    example=cv.startdate_yyyymmdd(),
    description='Chronicle contract start date.'
)
catalog.add_field(
    'priceplan_code',
    example=cv.priceplan_code(),
    description='Chronicle code of the priceplan'
)
catalog.add_field(
    'priceplan_price',
    example=cv.priceplan_price(),
    description='Chronicle price of the priceplan'
)
catalog.add_field(
    'status',
    example=0,
    description='Chronicle client status. 1 is active. 2 is inactive. 4 is discontinued.' # noqa E501
)
catalog.add_field(
    'customerid',
    example=cv.customer_id(),
    description='Chronicle client id'
)
