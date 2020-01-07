"""The Schema class. Used to handle fixtures and managed spark
schemas.
"""
from birgitta.schema.fixtures import ExampleVal
from birgitta.schema.nullable import Nullable
from birgitta.schema.spark import cast_schema, to_spark, to_escaped_spark  # noqa 401

NAME_POS = 0
TYPE_POS = 1
OTHER_POS = 2

__all__ = ['Schema', 'Nullable']


class Schema:
    def __init__(self, arr_schema, catalog):
        """Constructs a schema object which should be treated
        as immutable.

        Args:
            arr_schema (list): a schema in the list based simple schema DSL.

            E.g.
            [
                ["sequence_no", "string"],
                ["customer_id", "bigint"],
                ["phone", "string"]
            ]
            catalog (Catalog): the field catalog to get fixtures and
            descriptions from.
        """
        self.arr_schema = arr_schema
        self.catalog = catalog
        self.field_confs = self.get_field_confs()

    def to_spark(self):
        return to_spark(self.field_confs)

    def to_escaped_spark(self):
        return to_escaped_spark(self.arr_schema)

    def fields(self):
        return list(map(lambda f: f[NAME_POS], self.arr_schema))

    def types(self):
        return self.dict()

    def cast(self, df):
        return cast_schema(df, self)

    def example_val_override(self, field):
        field_conf = self.field_confs[field]
        if "example" in field_conf:
            return field_conf["example"]
        return None

    def field_params(self):
        ret = {}
        for row in self.arr_schema:
            field = row[0]
            if len(row) > 2:
                ret[field] = row[2]
            else:
                ret[field] = {}
        return ret

    def get_field_confs(self):
        """Convert list DSL into a dict of values:
        {
            "name": {
                "type": "str",
                "nullable": True
            },
            "customer_id": {
                "type": "str",
                "nullable": True,
                "example": 124 # if present
            }
            ...
        }
        """
        ret = {}
        for row in self.arr_schema:
            field = row[NAME_POS]
            conf = {
                "type": row[TYPE_POS],
                "nullable": True  # Same default nullable val as spark
            }
            num_fields = len(row)
            if num_fields > OTHER_POS:
                # Processs other arguments
                for i in range(OTHER_POS, num_fields):
                    other_arg = row[i]
                    arg_type = type(other_arg)
                    if arg_type == ExampleVal:
                        conf["example"] = ExampleVal
                    elif arg_type == Nullable:
                        conf["nullable"] = other_arg.val
            ret[field] = conf
        return ret

    def dict(self):
        return {k[NAME_POS]: k[TYPE_POS] for k in self.arr_schema}
