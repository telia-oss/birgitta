import re
from functools import partial

import pytest
from birgitta.dataframe import dataframe
from birgitta.dataframe import dfdiff
from birgitta.dataframe.dataframe import SchemaError
from birgitta.dataframesource.sources.localsource import LocalSource
from birgitta.fields.catalog import Catalog
from birgitta.schema.nullable import Nullable
from birgitta.schema.schema import Schema
from py4j.protocol import Py4JJavaError
from pyspark.sql import functions as F
from tests.datasets import fixtures, expected  # noqa F401


@pytest.fixture()
def dataframe_source(tmpdir):
    return LocalSource(dataset_dir=tmpdir.strpath)


@pytest.fixture()
def default_schema():
    catalog = Catalog()
    arr_schema = [['letter', 'string'],
                  ['number', 'bigint']]
    return Schema(arr_schema, catalog)


@pytest.fixture()
def wrong_types_schema():
    catalog = Catalog()
    arr_schema = [['letter', 'float'],
                  ['number', 'timestamp']]
    return Schema(arr_schema, catalog)


@pytest.fixture()
def wrong_fields_schema():
    catalog = Catalog()
    arr_schema = [['notletter', 'string'],
                  ['notnumber', 'bigint']]
    return Schema(arr_schema, catalog)


@pytest.fixture()
def wrong_num_fields_schema():
    catalog = Catalog()
    arr_schema = [['letter', 'string'],
                  ['number', 'bigint'],
                  ['foo', 'string', Nullable(False)]]
    return Schema(arr_schema, catalog)


@pytest.fixture()
def one_field_schema():
    catalog = Catalog()
    arr_schema = [['letter', 'string']]
    return Schema(arr_schema, catalog)


@pytest.fixture()
def wrong_nulls_schema():
    catalog = Catalog()
    arr_schema = [['letter', 'string', Nullable(False)],
                  ['number', 'bigint', Nullable(False)]]
    return Schema(arr_schema, catalog)


def assert_schema(spark_session,
                  dataset_name,
                  expected,  # noqa F401
                  dataframe_source,
                  schema):
    result = dataframe.get(spark_session,
                           dataset_name,
                           schema=schema,
                           dataframe_source=dataframe_source)
    assert not dfdiff.diff(result, expected)


def assertion_error(spark_session,
                    dataset_name,
                    expected,  # noqa F401
                    dataframe_source,
                    schema,
                    pattern):
    with pytest.raises(AssertionError) as e_info:
        assert_schema(spark_session,
                      dataset_name,
                      expected,
                      dataframe_source,
                      schema)
    assert re.match(pattern, str(e_info.value))


def test_get_with_schema(dataframe_source,
                         fixtures,  # noqa F401
                         expected,  # noqa F401
                         spark_session,
                         default_schema,
                         wrong_types_schema,
                         wrong_fields_schema,
                         wrong_num_fields_schema,
                         one_field_schema):
    dataframe_source.write(fixtures, 'assert_example')
    assert_partial = partial(assert_schema,
                             spark_session,
                             'assert_example',
                             expected,
                             dataframe_source)
    assertion_error_partial = partial(assertion_error,
                                      spark_session,
                                      'assert_example',
                                      expected,
                                      dataframe_source)
    assert_partial(default_schema)
    # Assert wrong types partial raising Py4J error
    with pytest.raises(Py4JJavaError):
        result = dataframe.get(spark_session,
                               'assert_example',
                               schema=wrong_types_schema,
                               dataframe_source=dataframe_source)
        result.collect()  # Force processing to trigger schema issues
    # Test assertion errors
    assertion_error_partial(wrong_fields_schema,
                            ".*Cols diff.*notletter,notnumber.*")
    assertion_error_partial(wrong_num_fields_schema,
                            ".*Cols diff.*Expected: letter,number,foo.*")
    assertion_error_partial(one_field_schema,
                            ".*Cols diff.*Expected: letter.*")


def test_get_with_schema_not_nullable(dataframe_source,
                                      fixtures,  # noqa F401
                                      expected,  # noqa F401
                                      spark_session,
                                      default_schema,
                                      wrong_nulls_schema):
    fixtures_w_nulls = fixtures.withColumn(
        'letter',
        F.when(F.col("letter") != 'a', F.col('letter')).otherwise(
            F.lit(None)))
    dataframe_source.write(fixtures_w_nulls, 'assert_example_not_nullable')
    dataframe.get(spark_session,
                  'assert_example_not_nullable',
                  schema=default_schema,
                  dataframe_source=dataframe_source)
    # This test currently does not raise a type error, since read with schema
    # doesn't raise an error on null values. Spark feature?
    dataframe.get(spark_session,
                  'assert_example_not_nullable',
                  schema=wrong_nulls_schema,
                  dataframe_source=dataframe_source)


def assert_write(dataframe_source,
                 spark_session,
                 df,
                 expected_df,  # noqa F401
                 dataset_name,
                 schema,
                 error_pattern=None):
    ds_name = "schema_assert_write_" + dataset_name
    if not error_pattern:
        dataframe.write(df,
                        ds_name,
                        schema=schema,
                        dataframe_source=dataframe_source)
    else:
        with pytest.raises(SchemaError) as e_info:
            dataframe.write(df,
                            ds_name,
                            schema=schema,
                            dataframe_source=dataframe_source)
        cleaned_err_val = str(
            e_info.value
        ).replace("\n", " ").replace("'", "")
        assert re.match(error_pattern, cleaned_err_val)


def test_write_with_schema(dataframe_source,
                         fixtures,  # noqa F401
                         expected,  # noqa F401
                         spark_session,
                         default_schema,
                         wrong_types_schema,
                         wrong_fields_schema,
                         wrong_num_fields_schema,
                         one_field_schema):
    assert_partial = partial(assert_write,
                             dataframe_source,
                             spark_session,
                             fixtures,
                             expected)
    assert_partial('default', default_schema)
    # No error for wrong types, because spark converts on cast
    assert_partial('wrong_types', wrong_types_schema)
    error_pattern = ".*Not in df.*0:notletter:type, 1:notnumber:type.*Not in schema.*"  # noqa F501
    assert_partial('wrong_fields', wrong_fields_schema, error_pattern)
    error_pattern = ".*Not in df.*0:letter:type, 1:number:type, 2:foo:type.*Not in schema.*"  # noqa F501
    assert_partial('wrong_num_fields', wrong_num_fields_schema, error_pattern)
    # No error for casting to single field
    assert_partial('one_field', one_field_schema, None)
