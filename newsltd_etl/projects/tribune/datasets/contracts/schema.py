from birgitta.schema.schema import Schema

from .....shared.schema.catalog.tribune import catalog


fields = [
    ["sequence_no", "string"],
    ["customer_id", "bigint"],
    ["phone", "string"],
    ["group_account_id", "bigint"],
    ["start_date", "timestamp"],
    ["end_date", "timestamp"],
    ["contract_prod_code", "string"],
    ["brand_code", "bigint"],
    ["segment", "string"],
    ["product", "string"],
    ["product_payment_type", "string"],
    ["product_name", "string"],
    ["brand_name", "string"],
    ["shop_code", "string"],
    ["sales_agent", "string"]
]

schema = Schema(fields, catalog)
