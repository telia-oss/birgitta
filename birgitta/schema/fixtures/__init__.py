"""Schema fixture functions.
"""
import datetime

import pandas as pd

__all__ = ['df', 'df_w_rows']


def df(spark, schema, row_confs=[{}]):
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
    return schema.enforce(df)


def rows(schema, row_confs):
    """Return rows based on row_confs"""
    entries = []
    for row_conf in row_confs:
        entries.append(row(schema, row_conf))
    return entries


def row(schema, fixture_confs):
    """Get a row based on the schema and fixture_confs.

    Returns a list of values.
    """
    ret = []
    schema_types = schema.types()
    for field in schema.fields():
        # The catalog example value conf can be overriden
        # in the fixture
        # or
        # in the schema.
        override_conf = fixture_confs.get(field)
        if not override_conf:
            override_conf = schema.example_val_override(field)
        val = schema.catalog.example_val(field, override_conf)
        field_type = schema_types[field]
        # Convert to datetime if field is timestamp and val is date,
        # to avoid explicit type-only overrides of example val in schema confs
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
