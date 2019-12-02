from birgitta.fields import Catalog
from birgitta.schema.fixtures import values as v

from ....shared.schema.fixtures.values import tribune as tv


catalog = Catalog()
catalog.add_field(
    'group_account_id',
    example=tv.group_account_id(),
    description="Brand code, 1 is Tribune. 44 is the bankrupt Times."
)
catalog.add_field(
    'brand_code',
    example=1,
    description="Brand code, 1 is Tribune. 44 is the bankrupt Times."
)
catalog.add_field(
    'brand_name',
    example='Tribune',
    description="Brand name, E.g. 'Tribune' or 'Times'."
)
catalog.add_field(
    'current_flag',
    example=1,
    description="Is this the latest/current version of this entry"
)
catalog.add_field(
    'datestamp',
    example=v.inthepast(),
    description="Date stamp"
)
catalog.add_field(
    'shop_code',
    example=tv.shop_code(),
    description="Dealer code"
)
catalog.add_field(
    'end_date',
    example=v.today_datetime(),
    description="Expiration (end) date"
)
catalog.add_field(
    'period_date',
    example=v.inthepast(),
    description="Period date"
)
catalog.add_field(
    'priceplan_code',
    example=tv.prod_code(),
    description="Price plan code"
)
catalog.add_field(
    'priceplan_name',
    example=tv.priceplan_name(),
    description="Price plan name"
)
catalog.add_field(
    'product',
    example='Regular Paper',
    description="Product, e.g. 'Regular Paper'"
)
catalog.add_field(
    'product_category',
    example='contract',
    description="Product category, e.g. 'contract'"
)
catalog.add_field(
    'product_code',
    example=tv.prod_code(),
    description="Product code"
)
catalog.add_field(
    'product_name',
    example=tv.priceplan_name(),
    description="Product name"
)
catalog.add_field(
    'product_payment_type',
    example='Paper',
    description="Product payment type"
)
catalog.add_field(
    'product_type',
    example='GSM',
    description="Product type"
)
catalog.add_field(
    'segment',
    example='Private',
    description="Segment e.g. 'Private'"
)
catalog.add_field(
    'prod_code',
    example=tv.prod_code(),
    description="NETV"
)
catalog.add_field(
    'sales_agent',
    example=tv.sales_agent(),
    description="Sales agent"
)
catalog.add_field(
    'start_date',
    example=v.inthepast(),
    description="Contract start date"
)
catalog.add_field(
    'sequence_no',
    example=tv.sec_no(),
    description="Contract sequence number"
)
catalog.add_field(
    'customer_id',
    example=tv.customer_id(),
    description="Customer ID. The primary identifier of customers (i.e. end users)" # noqa E501
)
catalog.add_field(
    'phone',
    example=tv.phone(),
    description="Phone number"
)
catalog.add_field(
    'client_status',
    example='active',
    description="Contract status"
)
catalog.add_field(
    'client_status_code',
    example=0,
    description="Contract status code"
)
catalog.add_field(
    'contract_prod_code',
    example=tv.prod_code(),
    description="Contract product code"
)
