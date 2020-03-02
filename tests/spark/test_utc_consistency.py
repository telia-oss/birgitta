"""Test that UTC is set correctly as spark timezone, and
that type casting to timestamp gives correct timezone.

We have one record for 2016-10-30, since it has a quirky bug
on certain spark versions/constellations.
The quirk is that 2016-10-30 might get an extra hour, under certain
conditions, like when using from_utc_timestamp().

If spark.sql.session.timeZone is set to Europe/Oslo, this the extra hour
is not added.

When converting a date string to a timestamp, we should use a simple cast()
instead of a trying to apply from_utc_timestamp()."""

import datetime
import pytest  # noqa F401
from birgitta.dataframe import dfdiff
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType


@pytest.fixture()
def expected(spark_session):
    expected_schema = StructType([
        StructField('f_dat', StringType()),
        StructField('c_dat', TimestampType())
    ])
    expected_data = [
        ['2016-10-30',
         datetime.datetime(2016, 10, 30, 0, tzinfo=datetime.timezone.utc)],
        ['2017-08-10',
         datetime.datetime(2017, 8, 10, 0, tzinfo=datetime.timezone.utc)]
    ]
    return spark_session.createDataFrame(expected_data, expected_schema)


def test_utc_consistency(spark_session, expected):
    """Ensure that UTC is set correctly as spark timezone, and
    that type casting to timestamp gives correct timezone.

    When converting a date string to a timestamp, it should use
    a simple cast().

    We have one record for 2016-10-30, since it has a quirky bug
    on certain spark versions/constellations.
    """
    assert spark_session.conf.get('spark.sql.session.timeZone') == 'UTC'

    rdd = spark_session.sparkContext.parallelize([
        Row(col1="2016-10-30"),
        Row(col1="2017-08-10")
    ])
    df = spark_session.createDataFrame(rdd)
    date_df = df.withColumnRenamed("col1", "dat")
    fcol = F.col("dat")
    ts_type = TimestampType()
    datetime_df = (
        date_df
        .withColumn('f_dat', F.date_format(fcol, 'yyyy-MM-dd'))
        .withColumn('c_dat', fcol.cast(ts_type))
        .drop('dat'))
    dfdiff.assert_equals(datetime_df, expected)
