from birgitta.fields.catalog import Catalog
from birgitta.schema.fixtures import values as v

from ....shared.schema.fixtures.values import tribune as tv


FIELD_CONFS = {
    'group_account_id': {
        "example": {"fn": tv.group_account_id},
        "description": "Billing Account Number"
    },
    'brand_code': {
        "example": {"static": 1},
        "description": "Brand code, 1 is Tribune. 44 is the bankrupt Times."
    },
    'current_flag': {
        "example": {"static": 1},
        "description": "Is this the latest/current version of this entry"
    },
    'datestamp': {
        "example": {"fn": v.inthepast},
        "description": "Date stamp"
    },
    'shop_code': {
        "example": {"fn": tv.shop_code},
        "description": "Dealer code"
    },
    'end_date': {
        "example": {"fn": v.today_datetime},
        "description": "Expiration (end) date"
    },
    'period_date': {
        "example": {"fn": v.inthepast},
        "description": "Period date"
    },
    'priceplan_code': {
        "example": {"fn": tv.prod_code},
        "description": "Price plan code"
    },
    'priceplan_name': {
        "example": {"fn": tv.priceplan_name},
        "description": "Price plan name"
    },
    'product': {
        "example": {"static": u'Regular Paper'},
        "description": "Product, e.g. 'Regular Paper'"
    },
    'product_category': {
        "example": {"static": u'contract'},
        "description": "Product category, e.g. 'contract'"
    },
    'product_code': {
        "example": {"fn": tv.prod_code},
        "description": "Product code"
    },
    'product_name': {
        "example": {"fn": tv.priceplan_name},
        "description": "Product name"
    },
    'product_payment_type': {
        "example": {"static": u'Paper'},
        "description": "Product payment type"
    },
    'product_type': {
        "example": {"static": u'GSM'},
        "description": "Product type"
    },
    'segment': {
        "example": {"static": u'Private'},
        "description": "Segment e.g. 'Private'"
    },
    'prod_code': {
        "example": {"fn": tv.prod_code},
        "description": "NETV"
    },
    'sales_agent': {
        "example": {"fn": tv.sales_agent},
        "description": "Sales agent"
    },
    'start_date': {
        "example": {"fn": v.inthepast},
        "description": "Contract start date"
    },
    'sequence_no': {
        "example": {"fn": tv.sec_no},
        "description": "Contract sequence number"
    },
    'customer_id': {
        "example": {"fn": tv.customer_id},
        "description": "Customer ID. The primary identifier of customers (i.e. end users)" # noqa 501
    },
    'phone': {
        "example": {"fn": tv.phone},
        "description": "Phone number"
    },
    'client_status': {
        "example": {"static": u'active'},
        "description": "Contract status"
    },
    'client_status_code': {
        "example": {"static": 0},
        "description": "Contract status code"
    },
    'contract_prod_code': {
        "example": {"fn": tv.prod_code},
        "description": "Contract product code"
    }
}

catalog = Catalog(FIELD_CONFS)
