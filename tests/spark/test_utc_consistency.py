import datetime
import pytest  # noqa F401
from birgitta.dataframe import dfdiff
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


@pytest.fixture()
def expected(spark_session):
    expected_schema = StructType([
        StructField('f_dat', StringType()),
        StructField('c_dat', TimestampType()),
        StructField('f_u_dat', TimestampType())
    ])
    expected_data = [
        ['2016-10-30',
         datetime.datetime(2016, 10, 30, 0, tzinfo=datetime.timezone.utc),
         datetime.datetime(2016, 10, 30, 1, tzinfo=datetime.timezone.utc)],
        ['2017-08-10',
         datetime.datetime(2017, 8, 10, 0, tzinfo=datetime.timezone.utc),
         datetime.datetime(2017, 8, 10, 0, tzinfo=datetime.timezone.utc)]
    ]
    return spark_session.createDataFrame(expected_data, expected_schema)


def test_utc_consistency(spark_session, expected):
    """Ensure that UTC is set correctly as spark timezone, by reproducing
    expected quirky bug of 2016-10-30 getting an extra hour.

    If spark.sql.session.timeZone is set to Europe/Oslo, this the extra hour
    is not added.
    This test then ensures that our default timezone options are consistent
    and that the extra hour bug is handled correctly in type conversions.

    When converting a date string to a timestamp, it should use a simple cast()
    instead of a trying to apply from_utc_timestamp().
    """
    assert spark_session.conf.get('spark.sql.session.timeZone') == 'UTC'

    rdd = spark_session.sparkContext.parallelize([
        Row(col1="2016-10-30"),
        Row(col1="2017-08-10")
    ])
    df = spark_session.createDataFrame(rdd)
    date_df = df.withColumnRenamed("col1", "dat")
    fcol = F.col("dat")
    f_date_format = F.date_format(fcol, 'yyyy-MM-dd')
    f_from_utc_ts = F.from_utc_timestamp(f_date_format, 'UTC')
    ts_type = TimestampType()
    datetime_df = (
        date_df
        .withColumn('f_dat', F.date_format(fcol, 'yyyy-MM-dd'))
        .withColumn('c_dat', fcol.cast(ts_type))
        .withColumn('f_u_dat', f_from_utc_ts)
        .drop('dat'))
    assert not dfdiff.diff(datetime_df, expected)
