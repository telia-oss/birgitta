from birgitta.schema.schema import Schema

from .....shared.schema.catalog.tribune_chronicle import catalog

fields = [
    ['customer_id', 'bigint'],
    ['phone', 'string'],
    ['chronicle_account_id', 'bigint'],
    ['group_account_id', 'bigint'],
    ['start_date', 'date'],
    ['end_date', 'date'],
    ['priceplan_code', 'string'],
    ['current_flag', 'bigint'],
    ['client_status_code', 'bigint']
]

schema = Schema(fields, catalog)
