from birgitta.schema.schema import Schema

from .....shared.schema.catalog.tribune import catalog

FIELDS = [
    ['datestamp', 'date'],
    ['customer_id', 'bigint'],
    ['product_code', 'string'],
    ['product', 'string'],
    ['product_name', 'string'],
    ['product_category', 'string'],
    ['brand_name', 'string'],
    ['segment', 'string'],
    ['start_date', 'date'],
    ['end_date', 'date'],
    ['shop_code', 'string']
]

schema = Schema(FIELDS, catalog)
