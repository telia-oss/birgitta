from birgitta.schema.schema import Schema

from .....shared.schema.catalog.ignatius import catalog

FIELDS = [
    ['accountid', 'bigint'],
    ['groupid', 'bigint'],
    ['customerid', 'bigint'],
    ['cellphone', 'string'],
    ['status', 'bigint'],
    ['startdate_yyyymmdd', 'string'],
    ['enddate_yyyymmdd', 'string'],
    ['priceplan_code', 'string'],
    ['priceplan_price', 'float']
]

schema = Schema(FIELDS, catalog)
