from unittest.mock import MagicMock

import pytest
from birgitta import spark
from birgitta.dataframe import dataframe, dfdiff
from birgitta.dataframesource.sources.s3source import S3Source  # noqa F401
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


@pytest.fixture(scope="session")
def spark_session():
    return spark.local_session()  # duration: about 3secs


fixtures_schema = StructType([
    StructField('letter', StringType()),
    StructField('number', IntegerType())
])


@pytest.fixture()
def fixtures_data():
    return [
        ['a', 1],
        ['b', 2],
        ['c', 3],
        ['d', 4],
        ['e', 5],
        ['f', 6],
        ['g', 7],
        ['h', 8],
        ['i', 9],
        ['j', 10]
    ]


@pytest.fixture()
def fixtures(spark_session, fixtures_data):
    fixtures = spark_session.createDataFrame(fixtures_data, fixtures_schema)
    return fixtures


@pytest.fixture()
def expected(spark_session, fixtures_data):
    expected = spark_session.createDataFrame(fixtures_data, fixtures_schema)
    return expected


@pytest.fixture()
def s3_source_load_ret(spark_session, fixtures_data, expected):
    return spark_session.createDataFrame(fixtures_data, fixtures_schema)


def test_equal(spark_session, fixtures, expected, s3_source_load_ret):
    s3_source = S3Source(format='parquet')
    s3_source.write = MagicMock(return_value=None)
    s3_source.load = MagicMock(return_value=s3_source_load_ret)
    dataset_name = "fixtures"
    s3_dir = "s3://birgittatestbucket/sourcetests"
    dataframe.write(fixtures,
                    dataset_name,
                    prefix=s3_dir,
                    dataframe_source=s3_source)
    out_df = dataframe.get(spark_session,
                           dataset_name,
                           prefix=s3_dir,
                           dataframe_source=s3_source)
    assert not dfdiff.diff(out_df, expected)
