"""The Schema class. Used to handle fixtures and managed spark
schemas.
"""
from birgitta.schema.spark import enforce_schema, to_spark, to_escaped_spark  # noqa 401

NAME_POS = 0
TYPE_POS = 1

__all__ = ['Schema']


class Schema:
    def __init__(self, arr_schema, catalog):
        """Constructs a schema object which should be treated
        as immutable.

        Args:
            arr_schema (list): a schema in the list based simple schema DSL.
            catalog (Catalog): the field catalog to get fixtures and
            descriptions from.
        """
        self.arr_schema = arr_schema
        self.catalog = catalog

    def to_spark(self):
        return to_spark(self.arr_schema)

    def to_escaped_spark(self):
        return to_escaped_spark(self.arr_schema)

    def fields(self):
        return list(map(lambda f: f[NAME_POS], self.arr_schema))

    def types(self):
        return self.dict()

    def enforce(self, df):
        return enforce_schema(df, self)

    def example_val_override(self, field):
        return self.field_params()[field]

    def field_params(self):
        ret = {}
        for row in self.arr_schema:
            field = row[0]
            if len(row) > 2:
                ret[field] = row[2]
            else:
                ret[field] = {}
        return ret

    def dict(self):
        return {k[NAME_POS]: k[TYPE_POS] for k in self.arr_schema}
