"""Spark wrapper functions to enable abstractions and testing.
"""
import pyspark
from birgitta import timing
from birgitta.dataframe import storage
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


__all__ = ['local_session', 'sql_ctx']


def local_session():
    """Get a local spark session. Used for recipe tests,
    both running them, and creating fixtures."""
    conf = conf_spark()
    # Sets the Spark master URL to connect to, such as:
    #
    #   "local" to run locally,
    #   "local[4]" to run locally with 4 cores,
    #   local[*] Run Spark locally with as many worker threads as logical cores
    #   on your machine,
    #   "spark://89.9.250.25:7077" or "spark://master:7077" to run on a Spark
    #   standalone cluster.
    master_spark_url = 'local[*]'
    session = (SparkSession.builder
               .config(conf=conf)
               .master(master_spark_url)
               .appName('spark_test')
               .getOrCreate())
    timing.time("spark.local_session created/gotten")
    return session


def sql_ctx(spark_session=None):
    """Get an sql context. Currently, we support:

    * local session
    * dataiku
    """
    if is_local():
        return local_sql_ctx(spark_session)
    elif spark_session:
        return make_sql_ctx(spark_session)
    return dataiku_sql_ctx()


def make_sql_ctx(spark_session):
    sc = spark_session.sparkContext
    return SQLContext(sc)


def is_local():
    return storage.stored_in("MEM") or storage.stored_in("FILE")


def conf_spark():
    """Configure local spark to be fast for recipe tests."""
    pyspark.SparkConf().getAll()
    # Update the default configurations, for higher speed
    conf = pyspark.SparkConf().setAll([
        # No parallelism needed in small data
        ('spark.sql.shuffle.partitions', 1),
        #
        # No speed improvement from any of these settings so far:
        # ('spark.sql.execution.arrow.enabled', 'true')
        # ("spark.executor.memoryOverhead", "512m"),
        # ("spark.dynamicAllocation.maxExecutors", "10"),
        # ("spark.dynamicAllocation.enabled","false"),
        # ('spark.executor.memory', '512m'),
        # ('spark.executor.cores', '2'),
        # ('spark.cores.max', '8'),
        # ('spark.driver.memory', '512m')
    ])
    return conf


def local_sql_ctx(spark_session):
    if not spark_session:
        spark_session = local_session()
    return make_sql_ctx(spark_session)


def dataiku_sql_ctx():
    sc = pyspark.SparkContext.getOrCreate()
    return SQLContext(sc)
