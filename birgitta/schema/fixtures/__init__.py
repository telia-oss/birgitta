"""Schema fixture functions.
"""
import datetime

import pandas as pd

from .example_val import ExampleVal
from .fixture import Fixture
from .row_conf import RowConf

__all__ = ['df',
           'df_w_rows',
           'ExampleVal',
           'Fixture',
           'RowConf',
           'get_val']


def df(spark, schema, row_confs):
    """Returns a dataframe based on a schema and row conf.

    Args:
        spark (SparkContext): Spark context to generate data frame
        schema(Schema): Schema to create the data frame
        rows(list): List of row_conf dicts, one for each row.
    """
    return df_w_rows(spark, schema, rows(schema, row_confs))


def df_w_rows(spark, schema, rows):
    """Returns a dataframe based on rows.

    Args:
        spark (SparkContext): Spark context to generate data frame
        schema(Schema): Schema to create the data frame
        rows(list): List of values
    """
    convert_to_pandas = False
    if convert_to_pandas:  # Not working yet
        if rows:
            rows = pd.DataFrame.from_records(rows)
    # Escape date time fields to avoid type problems between pandas and pyarrow
    df = spark.createDataFrame(rows, schema.to_escaped_spark())
    return schema.cast(df)


def rows(schema, row_confs):
    """Return rows based on row_confs"""
    entries = []
    for row_conf in row_confs:
        entries.append(row(schema, row_conf))
    return entries


def get_val(schema, row_conf, field):
    # The catalog example value conf can be overriden:
    # in the fixture
    # or
    # in the schema.
    val = row_conf.get_field(field)  # fixture
    if val is None:
        val = schema.example_val_override(field)  # schema
    if val is None:
        val = schema.catalog.example_val(field)  # catalog
    return val


def row(schema, row_conf):
    """Get a row based on the schema and row_confs.

    Returns a list of values.
    """
    ret = []
    schema_types = schema.types()
    for field in schema.fields():
        val = get_val(schema, row_conf, field)
        # Convert to datetime if field is timestamp and val is date,
        # to avoid explicit type-only overrides of example val in schema confs
        field_type = schema_types[field]
        if (
                field_type == 'timestamp' and
                (type(val) == datetime.date)):
            val = date_to_datetime(val)
        if type(val) == datetime.date:
            val = val.strftime('%Y-%m-%d')
        if type(val) == datetime.datetime:
            val = val.strftime('%Y-%m-%d %H:%M:%S')
        ret.append(val)
    return ret


def date_to_datetime(d):
    """Convert date values to date time to avoid type errors.
    """
    return datetime.datetime.combine(d, datetime.datetime.min.time())
