import pytest
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


fixtures_schema = StructType([
    StructField('letter', StringType()),
    StructField('number', LongType())
])


fixtures_data = [
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
def fixtures(spark_session):
    fixtures = spark_session.createDataFrame(fixtures_data, fixtures_schema)
    return fixtures


@pytest.fixture()
def expected(spark_session):
    expected = spark_session.createDataFrame(fixtures_data, fixtures_schema)
    return expected
