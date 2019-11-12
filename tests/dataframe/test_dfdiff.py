import pytest
from birgitta import spark
from birgitta.dataframe import dfdiff
from pyspark.sql import functions as F
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


def test_equal(fixtures, expected):
    assert not dfdiff.diff(fixtures, expected)


def test_error_on_short(fixtures, expected):
    expected_short = expected.limit(5)
    assert dfdiff.diff(fixtures, expected_short) == """Error: Row count diff
        Expected: 10
        Actual:   5"""


def test_error_on_extra_col(fixtures, expected):
    expected_extra_col = expected.withColumn("foo", F.lit("bar"))
    assert dfdiff.diff(fixtures, expected_extra_col) == """Error: Cols diff
        Expected: letter,number
        Actual:   letter,number,foo"""


def test_error_on_col_name_diff(fixtures, expected):
    expected_col_name_diff = expected.withColumn(
        'numfoo', F.col('number')
    ).drop('number')
    assert dfdiff.diff(fixtures, expected_col_name_diff) == """Error: Cols diff
        Expected: letter,number
        Actual:   letter,numfoo"""


def test_error_on_val_diff(fixtures, expected):
    expected_val_diff = expected.withColumn('number', F.lit(3))
    assert dfdiff.diff(fixtures, expected_val_diff) == """Error: Rows are different
        Only in expected:
          letter  number
0      a       1
1      b       2
2      d       4
3      e       5
4      f       6
5      g       7
6      h       8
7      i       9
8      j      10
        Only in actual result:
          letter  number
0      a       3
1      b       3
2      d       3
3      e       3
4      f       3
5      g       3
6      h       3
7      i       3
8      j       3
        Expected:
          letter  number
0      a       1
1      b       2
2      c       3
3      d       4
4      e       5
5      f       6
6      g       7
7      h       8
8      i       9
9      j      10
        Actual:
          letter  number
0      a       3
1      b       3
2      c       3
3      d       3
4      e       3
5      f       3
6      g       3
7      h       3
8      i       3
9      j       3"""


def test_error_on_single_val_diff(fixtures, expected):
    num_when = F.when(F.col('number') == 3, 3333).otherwise(F.col('number'))
    expected_single_val_diff = expected.withColumn('number', num_when)
    assert dfdiff.diff(fixtures, expected_single_val_diff) == """Error: Rows are different
        Only in expected:
          letter  number
0      c       3
        Only in actual result:
          letter  number
0      c    3333
        Expected:
          letter  number
0      a       1
1      b       2
2      c       3
3      d       4
4      e       5
5      f       6
6      g       7
7      h       8
8      i       9
9      j      10
        Actual:
          letter  number
0      a       1
1      b       2
2      c    3333
3      d       4
4      e       5
5      f       6
6      g       7
7      h       8
8      i       9
9      j      10"""
