import pytest  # noqa F401
from birgitta.dataframe import dfdiff
from pyspark.sql import functions as F
from tests.datasets import fixtures, expected  # noqa F




def test_equal(fixtures, expected):  # noqa F811
    assert not dfdiff.diff(fixtures, expected)


def test_error_on_short(fixtures, expected):  # noqa F811
    expected_short = expected.limit(5)
    expected_output = """Error: Row count diff
Expected: 10
Actual:   5

Rows are different (max 20 rows shown)
Only in expected:
  letter  number
0      f       6
1      g       7
2      h       8
3      i       9
4      j      10
Only in actual result:
Empty DataFrame
Columns: [letter, number]
Index: []
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
2      c       3
3      d       4
4      e       5"""
    assert dfdiff.diff(fixtures, expected_short) == expected_output


def test_error_on_extra_col(fixtures, expected):  # noqa F811
    expected_extra_col = expected.withColumn("foo", F.lit("bar"))
    assert dfdiff.diff(fixtures, expected_extra_col) == """Error: Cols diff
Expected: letter,number
Actual:   letter,number,foo"""


def test_error_on_col_name_diff(fixtures, expected):  # noqa F811
    expected_col_name_diff = expected.withColumn(
        'numfoo', F.col('number')
    ).drop('number')
    assert dfdiff.diff(fixtures, expected_col_name_diff) == """Error: Cols diff
Expected: letter,number
Actual:   letter,numfoo"""


def test_error_on_val_diff(fixtures, expected):  # noqa F811
    expected_val_diff = expected.withColumn('number', F.lit(3))
    assert dfdiff.diff(fixtures, expected_val_diff) == """Error: Rows are different (max 20 rows shown)
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


def test_error_on_single_val_diff(fixtures, expected):  # noqa F811
    num_when = F.when(F.col('number') == 3, 3333).otherwise(F.col('number'))
    expected_single_val_diff = expected.withColumn('number', num_when)
    assert dfdiff.diff(fixtures, expected_single_val_diff) == """Error: Rows are different (max 20 rows shown)
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
