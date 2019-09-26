from birgitta.schema.fixtures import values as v
from birgitta.schema.schema import Schema

from .....shared.schema.catalog.tribune import catalog

FIELDS = [
    ["sequence_no", "bigint"],
    ["customer_id", "bigint"],
    ["phone", "string"],
    ["group_account_id", "bigint"],
    ["product_code", "string"],
    ["product", "string"],
    ["segment", "string"],
    ["product_payment_type", "string"],
    ["product_name", "string"],
    ["brand_name", "string"],
    ["start_date", "date"],
    ["end_date", "date", {"example": {"fn": v.today}}],
    ["shop_code", "string"],
    ["product_category", "string"]
]

schema = Schema(FIELDS, catalog)
