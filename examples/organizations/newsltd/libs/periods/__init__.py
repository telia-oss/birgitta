import datetime
from datetime import timedelta

from pyspark.sql import functions as F
from pyspark.sql import types as T

__all__ = ['group_by_day']


def group_by_day(df,
                 *,
                 spark_session,
                 group_keys,
                 from_date_field,
                 to_date_field,
                 join_partitions=100,
                 count_partitions=3,
                 min_cutoff_date=None,
                 max_cutoff_date=datetime.date.today(),
                 drop_date_fields=True):
    """Count entries per day grouped by the keys in group_keys.

    Args:
        df (DataFrame): Incoming spark dataframe.

    Kwargs:
        spark_session (SparkSession): Spark session used to build date
        dataframe.
        group_keys (list): List of strings with keys to group by.
        from_date_field (str): From data field name.
        to_date_field (str): To data field name.
        join_partitions (int): Number of partitions for the join with dates.
        count_partitions (int): Number of partitions after the count.
        min_cutoff_date (datetime.date): Drop all data before this date.
        max_cutoff_date (datetime.date): Drop all data after this date,
        can often be today.
        drop_date_fields (bool): If True drop original from_date and to_date
        fields from result.
    Returns:
       Spark DataFrame with grouped counts per key combination per day.

    Example input:
    DataFrame
    +-------------+---------+-----------+-----------+---------+--------+
    |customer_id  |prod_code|shop_code  |channel    |from_date| to_date|
    +-------------+---------+-----------+-----------+---------+--------+
    |         1234|      ABC|      SHOP1|          A| 20160606|20160610|
    |         3456|      ABC|      SHOP1|          A| 20160607|20160611|
    +-------------+---------+-----------+-----------+---------+--------+
    group_keys=['prod_code', 'shop_code', 'channel']
    from_date_field='from_date'
    to_date_field='to_date'
    drop_date_fields=True

    Example output:
    DataFrame
    +--------+-----------+------------+---------+-----+
    | datenum|channel    |shop_code   |prod_code|count|
    +--------+-----------+------------+---------+-----+
    |20160606|          A|       SHOP1|      ABC|    1|
    |20160607|          A|       SHOP1|      ABC|    2|
    +--------+-----------+------------+---------+-----+
    """
    df_with_dates = df\
        .withColumn('date_from', F.col(from_date_field))\
        .withColumn('date_to', F.col(to_date_field))

    dates_range = df_with_dates.agg(
        F.min(F.col("date_from")),
        F.max(F.col("date_to"))
    ).collect()

    all_dates = all_dates_between(dates_range[0][0],
                                  dates_range[0][1],
                                  min_cutoff_date,
                                  max_cutoff_date)
    dates_df = spark_session.createDataFrame(all_dates, T.DateType())
    between = dates_df['value'].between(df_with_dates['date_from'],
                                        df_with_dates['date_to'])
    result = df_with_dates.repartition(join_partitions)\
                          .join(dates_df, on=between)

    datenum_type = F.date_format(F.col('datenum'), 'YYYYMMdd').cast("int")
    result = result\
        .withColumn('datenum', result['value'])\
        .withColumn('datenum', datenum_type)\
        .drop('value')\
        .drop('date_from')\
        .drop('date_to')

    if drop_date_fields:
        result = result.drop(from_date_field).drop(to_date_field)

    new_group_keys = group_keys[:]
    new_group_keys.append('datenum')
    return result.groupBy(new_group_keys)\
                 .count().repartition(count_partitions)


def all_dates_between(ds_min_date,
                      ds_max_date,
                      min_cutoff_date,
                      max_cutoff_date):
    ds_min_date = ensure_date(ds_min_date)
    ds_max_date = ensure_date(ds_max_date)
    if min_cutoff_date:
        min_date = max(ds_min_date, min_cutoff_date)
    else:
        min_date = ds_min_date
    if max_cutoff_date:
        max_date = min(ds_max_date, max_cutoff_date)
    else:
        max_date = ds_max_date

    delta = max_date - min_date
    all_dates = []
    for i in range(delta.days + 1):
        day = min_date + timedelta(days=i)
        all_dates.append(day)
    return all_dates


def ensure_date(dt):
    if type(dt) == datetime.datetime:
        return dt.date()
    return dt
