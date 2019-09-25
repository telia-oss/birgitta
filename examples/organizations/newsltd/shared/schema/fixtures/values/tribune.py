def phone(index=0):
    return u"G479999999%d" % (index)


def customer_id(index=0):
    return 11220 + index


def group_account_id(index=0):
    return 3335412 + index


def sec_no(index=0):
    return 2233444 + index


def shop_code():
    return u'ONLINE'


def priceplan_name():
    return u'Regular paper edition'


def sales_agent(index=0):
    agents = [
        'A',
        'B',
        'C'
    ]
    return agents[index]


def prod_code():
    return u'PAPERVERSION'
