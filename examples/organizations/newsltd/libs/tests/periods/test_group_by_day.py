import datetime

import pytest
from birgitta import spark
from birgitta.dataframe import dfdiff
from pyspark.sql.types import DateType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

from ....libs import periods


@pytest.fixture(scope="session")
def spark_session():
    return spark.local_session()


fixtures_schema = StructType([
    StructField('customer_id', StringType()),
    StructField('prod_code', StringType()),
    StructField('shop_code', StringType()),
    StructField('channel', StringType()),
    StructField('from_date', DateType()),
    StructField('to_date', DateType()),
])


@pytest.fixture()
def min_date():
    return datetime.date(2019, 10, 1)


@pytest.fixture()
def max_date():
    return datetime.date(2019, 10, 3)


@pytest.fixture()
def fixtures_data(min_date):
    return [
        ['1234',
         'ABC',
         'SHOP1',
         'CHAN55',
         datetime.date(2016, 6, 9),
         datetime.date(2019, 10, 2)],
        ['1235',
         'ABC',
         'SHOP1',
         'CHAN77',
         datetime.date(2016, 6, 9),
         datetime.date(2019, 10, 29)],
        ['1237',
         'ABC',
         'SHOP1',
         'CHAN77',
         min_date + datetime.timedelta(days=1),
         datetime.date(2019, 10, 29)]
    ]


@pytest.fixture()
def fixtures(spark_session, fixtures_data):
    fixtures = spark_session.createDataFrame(fixtures_data, fixtures_schema)
    return fixtures


expected_schema = StructType([
    StructField('prod_code', StringType()),
    StructField('shop_code', StringType()),
    StructField('channel', StringType()),
    StructField('datenum', IntegerType()),
    StructField('count', LongType())
])


@pytest.fixture()
def expected_data(min_date, max_date):
    return [
        ['ABC', 'SHOP1', 'CHAN55', 20191001, 1],
        ['ABC', 'SHOP1', 'CHAN55', 20191002, 1],
        ['ABC', 'SHOP1', 'CHAN77', 20191001, 1],
        ['ABC', 'SHOP1', 'CHAN77', 20191002, 2],
        ['ABC', 'SHOP1', 'CHAN77', 20191003, 2]
    ]


@pytest.fixture()
def expected(spark_session, expected_data):
    expected = spark_session.createDataFrame(expected_data, expected_schema)
    return expected


def test_group_by_day(spark_session, fixtures, expected, min_date, max_date):
    group_keys = ['prod_code', 'shop_code', 'channel']
    from_date_field = 'from_date'
    to_date_field = 'to_date'
    grouped = periods.group_by_day(fixtures,
                                   group_keys=group_keys,
                                   from_date_field=from_date_field,
                                   to_date_field=to_date_field,
                                   join_partitions=1,
                                   count_partitions=1,
                                   min_cutoff_date=min_date,
                                   max_cutoff_date=max_date,
                                   spark_session=spark_session,
                                   drop_date_fields=True)
    dfdiff.assert_equals(grouped, expected)
