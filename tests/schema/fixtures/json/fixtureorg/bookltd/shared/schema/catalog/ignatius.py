from birgitta.fields.catalog import Catalog

from ...schema.fixtures.values import ignatius as cv


FIELD_CONFS = {
    'groupid': {
        'description': 'Ignatius group id', # noqa 501
        'example': {'fn': cv.groupid}
    },
    'accountid': {
        'description': 'Ignatius account id', # noqa 501
        'example': {'fn': cv.accountid}
    },
    'ignatius_account_id': {
        'description': 'Ignatius account id', # noqa 501
        'example': {'fn': cv.accountid}
    },
    'cellphone': {
        'description': 'Ignatius phone number', # noqa 501
        'example': {'fn': cv.cellphone}
    },
    'enddate_yyyymmdd': {
        'description': 'Ignatius contract end date.', # noqa 501
        'example': {'fn': cv.enddate_yyyymmdd}
    },
    'startdate_yyyymmdd': {
        'description': 'Ignatius contract start date.', # noqa 501
        'example': {'fn': cv.startdate_yyyymmdd}
    },
    'priceplan_code': {
        'description': 'Ignatius code of the priceplan', # noqa 501
        'example': {'fn': cv.priceplan_code}
    },
    'priceplan_price': {
        'description': 'Ignatius price of the priceplan', # noqa 501
        'example': {'fn': cv.priceplan_price}
    },
    'status': {
        'description': 'Ignatius client status. 1 is active. 2 is inactive. 4 is discontinued.', # noqa 501
        'example': {'static': 0}
    },
    'customerid': {
        'description': 'Ignatius client id', # noqa 501
        'example': {'fn': cv.customer_id}
    }
}

catalog = Catalog(FIELD_CONFS)
