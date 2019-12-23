"""Schema management and utilities for spark.
The functions can be used directly from notebooks.
"""
from functools import reduce

from pyspark.sql import types as t

__all__ = [
    'simple_to_spark',
    'to_spark',
    'to_escaped_spark',
    'from_spark_df',
    'cast_schema',
    'print_df_rows'
]


spark_short_to_class_types = {
    "bool": t.BooleanType,
    "bigint": t.IntegerType,
    "long": t.LongType,
    "float": t.FloatType,
    "double": t.DoubleType,
    "string": t.StringType,
    "timestamp": t.TimestampType,
    "date": t.DateType
}

spark_class_to_short_types = {
    "BooleanType": "bool",
    "LongType": "bigint",
    "IntegerType": "bigint",
    "FloatType": "float",
    "DoubleType": "double",
    "StringType": "string",
    "TimestampType": "timestamp",
    "DateType": "date"
}

NAME_POS = 0
TYPE_POS = 1


def simple_to_spark(simple_schema):
    fields = []
    for col in simple_schema:
        spark_type = short_to_class_type(col[TYPE_POS])
        fields.append(t.StructField(col[NAME_POS], spark_type()))
    return t.StructType(fields)


def to_spark(field_confs):
    fields = []
    for field in field_confs.keys():
        field_conf = field_confs[field]
        spark_type = short_to_class_type(field_conf['type'])
        fields.append(t.StructField(field,
                                    spark_type(),
                                    field_conf['nullable']))
    return t.StructType(fields)


def to_escaped_spark(simple_schema):
    """Escape date time fields to avoid type problems
    between pandas and pyarrow.

    Args:
        simple_schema (list): Is a schema in the simple dsl structure.

    Returns:
        Spark schema, where all date time types are converted to string.
    """
    fields = []
    for col in simple_schema:
        spark_type = short_to_class_type(col[TYPE_POS])
        if spark_type in [t.DateType, t.TimestampType]:
            spark_type = t.StringType
        fields.append(t.StructField(col[NAME_POS], spark_type()))
    return t.StructType(fields)


def from_spark_df(df):
    """Obtain the simple schema from a spark dataframe.

    Utility function to be used from a notebook.
    """
    return [[f.name, obj_to_short_type(f.dataType)] for f in df.schema.fields]


def print_df_rows(df, num_rows=2):
    """Print rows in python format from a spark dataframe.

    Utility function to be used from a notebook.
    """
    fields = df.schema.fields
    rows = df.take(num_rows)
    print("[")
    first_row = True
    for row in rows:
        if first_row:
            print("    {", end="")
            first_row = False
        else:
            print(",\n    {", end="")
        first_field = True
        for field in fields:
            if first_field:
                first_field = False
                print("\n        ", end="")
            else:
                print(",\n        ", end="")
            print("'%s': " % (field.name), repr(row[field.name]), end="")
        print("\n    }", end="")
    print("\n]")


def cast_schema(df, schema):
    """Enforce the schema on the data frame.

    Args:
        df (DataFrame): The dataframe.
        schema (list): The simple schema to be applied.

    Returns:
        A data frame converted to the schema.
    """
    spark_schema = schema.to_spark()
    # return spark_session.createDataFrame(df.rdd, schema=spark_schema)
    return reduce(cast,
                  spark_schema.fields,
                  df.select(*spark_schema.fieldNames()))


def short_to_class_type(short_type):
    return spark_short_to_class_types[short_type]


def cast(df, field):
    return df.withColumn(field.name, df[field.name].cast(field.dataType))


def obj_to_short_type(type_obj):
    return spark_class_to_short_types[type_obj.__class__.__name__]
