from birgitta.schema.fixtures import values as v
from birgitta.schema.fixtures.values import dtvals


def priceplan_price():
    return 103.2


def priceplan_code():
    return 'C574'


def customer_id(index=0):
    return 11220 + index


def accountid():
    return 6669999


def groupid():
    return 1111222


def cellphone():
    return "99999990"


def startdate_yyyymmdd():
    """We have no data before 2019-01-07"""
    return dtvals.dtstr(v.inthepast())


def enddate_yyyymmdd():
    """We have no data before 2019-01-07"""
    return dtvals.dtstr(v.today())
