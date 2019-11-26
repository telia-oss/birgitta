from birgitta.fields.catalog import Catalog

from ...schema.fixtures.values import chronicle as cv


FIELD_CONFS = {
    'groupid': {
        'description': 'Chronicle group id', # noqa 501
        'example': {'fn': cv.groupid}
    },
    'accountid': {
        'description': 'Chronicle account id', # noqa 501
        'example': {'fn': cv.accountid}
    },
    'chronicle_account_id': {
        'description': 'Chronicle account id', # noqa 501
        'example': {'fn': cv.accountid}
    },
    'cellphone': {
        'description': 'Chronicle phone number', # noqa 501
        'example': {'fn': cv.cellphone}
    },
    'enddate_yyyymmdd': {
        'description': 'Chronicle contract end date.', # noqa 501
        'example': {'fn': cv.enddate_yyyymmdd}
    },
    'startdate_yyyymmdd': {
        'description': 'Chronicle contract start date.', # noqa 501
        'example': {'fn': cv.startdate_yyyymmdd}
    },
    'priceplan_code': {
        'description': 'Chronicle code of the priceplan', # noqa 501
        'example': {'fn': cv.priceplan_code}
    },
    'priceplan_price': {
        'description': 'Chronicle price of the priceplan', # noqa 501
        'example': {'fn': cv.priceplan_price}
    },
    'status': {
        'description': 'Chronicle client status. 1 is active. 2 is inactive. 4 is discontinued.', # noqa 501
        'example': {'static': 0}
    },
    'customerid': {
        'description': 'Chronicle client id', # noqa 501
        'example': {'fn': cv.customer_id}
    }
}

catalog = Catalog(FIELD_CONFS)
