"""Schema converter for dataiku.
"""

__all__ = [
    'to_dataiku'
]


# See https://doc.dataiku.com/dss/latest/schemas/definitions.html
# string
# int (32 bits), bigint (64 bits), smallint (16 bits), tinyint (8 bits)
# float (32 bits decimal), double (64 bits decimal)
# boolean
# date
SHORT_TO_DKU_TYPE = {
    "bool": "boolean",
    "bigint": "bigint",
    "long": "bigint",
    "int": "int",
    "float": "float",
    "double": "double",
    "string": "string",
    "timestamp": "date",
    "date": "date"
}

# See https://doc.dataiku.com/dss/latest/schemas/meanings-list.html
SHORT_TO_DKU_MEANING = {
    "bool": "Boolean",
    "bigint": "Integer",
    "long": "Integer",
    "int": "Integer",
    "float": "Decimal",
    "double": "Decimal",
    "string": "Text",
    "timestamp": "Date",
    "date": "Date"
}


def to_dataiku(schema):
    """Convert the birgitta schema to a dataiku schema.

       We ignore birgitta conf Nullable, since not supported by Dataiku

    Args:
        schema (birgitta.schema): Schema to be converted into dataiku schema.

    Returns:
       Dataiku schema. E.g:

    {
        'userModified': True,
        'columns': [
            {'name': 'foobool', 'type': 'bool',
             'comment': 'Foo bool', 'meaning': 'Boolean'},
            {'name': 'fooint', 'type': 'bigint',
             'comment': 'Foo bigint', 'meaning': 'Integer'},
            {'name': 'foodate', 'type': 'date',
             'comment': 'Foo date', 'meaning': 'Date'},
        ]
    }
    """
    field_confs = schema.get_field_confs()
    columns = []
    for field in field_confs.keys():
        field_conf = field_confs[field]
        catalog_conf = schema.catalog.get_field_conf(field)
        column = {
            'name': field,
            'type': short_to_dku_type(field_conf['type']),
            'comment': catalog_conf['description'],
            'meaning': short_to_dku_meaning(field_conf['type']),
        }
        columns.append(column)

    return {
        'userModified': True,
        'columns': columns
    }


def short_to_dku_type(short_type):
    return SHORT_TO_DKU_TYPE[short_type]


def short_to_dku_meaning(short_type):
    return SHORT_TO_DKU_MEANING[short_type]
