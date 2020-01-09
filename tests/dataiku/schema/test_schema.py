import datetime

import pytest  # noqa 401
from birgitta.dataiku import schema as dkuschema
from birgitta.fields import Catalog
from birgitta.schema.schema import Schema


def test_to_dataiku():
    """This is necessary to ensure that row_conf field val 0 is not
    treated as None when deriving the fixture val."""
    catalog = Catalog()
    catalog.add_field('foobool', description='Foo bool', example=True)
    catalog.add_field('foobigint', description='Foo bigint', example=3999999)
    catalog.add_field('fooint', description='Foo int', example=39)
    catalog.add_field('foolong', description='Foo long', example=399)
    catalog.add_field('foofloat', description='Foo float', example=22.2)
    catalog.add_field('foodouble', description='Foo double', example=22.2)
    catalog.add_field('foostring', description='Foo string', example="foo")
    catalog.add_field('footimestamp', description='Foo timestamp',
                      example=datetime.datetime.now())
    catalog.add_field('foodate', description='Foo date',
                      example=datetime.date.today())
    schema_list = [
        ['foobool', 'bool'],
        ['foobigint', 'bigint'],
        ['fooint', 'int'],
        ['foolong', 'long'],
        ['foofloat', 'float'],
        ['foodouble', 'double'],
        ['foostring', 'string'],
        ['footimestamp', 'timestamp'],
        ['foodate', 'date']
    ]
    schema = Schema(schema_list, catalog)
    expected_schema = {
        'userModified': True,
        'columns': [
            {'name': 'foobool', 'type': 'boolean',
             'comment': 'Foo bool', 'meaning': 'Boolean'},
            {'name': 'foobigint', 'type': 'bigint',
             'comment': 'Foo bigint', 'meaning': 'Integer'},
            {'name': 'fooint', 'type': 'int',
             'comment': 'Foo int', 'meaning': 'Integer'},
            {'name': 'foolong', 'type': 'bigint',
             'comment': 'Foo long', 'meaning': 'Integer'},
            {'name': 'foofloat', 'type': 'float',
             'comment': 'Foo float', 'meaning': 'Decimal'},
            {'name': 'foodouble', 'type': 'double',
             'comment': 'Foo double', 'meaning': 'Decimal'},
            {'name': 'foostring', 'type': 'string',
             'comment': 'Foo string', 'meaning': 'Text'},
            {'name': 'footimestamp', 'type': 'date',
             'comment': 'Foo timestamp', 'meaning': 'Date'},
            {'name': 'foodate', 'type': 'date',
             'comment': 'Foo date', 'meaning': 'Date'},
        ]
    }
    dataiku_schema = dkuschema.to_dataiku(schema)
    assert dataiku_schema == expected_schema
