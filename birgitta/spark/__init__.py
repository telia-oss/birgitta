"""Spark wrapper functions to enable abstractions and testing.
"""
import pyspark
from birgitta import context
from birgitta import timing
from pyspark.sql import SparkSession


__all__ = ['local_session', 'session', 'default_server_session']


def session(*, conf=None, app_name=None):
    """Get a local spark session. Used for recipe tests,
    both running them, and creating fixtures.

    Timezone is set to UTC by default.
    """
    if not conf:  # Get default local spark conf
        conf = (pyspark.SparkConf()
                .set("spark.sql.session.timeZone", "UTC"))
    if not app_name:
        app_name = 'default_spark_app'

    if is_local():
        return local_session(app_name=app_name)
    else:
        return default_server_session(conf=conf)


def default_server_session(*, conf):
    """Don't override app_name, since context might have given it
    a useful name."""
    session = (SparkSession.builder
               .config(conf=conf)
               .getOrCreate())
    timing.time("spark.default_server_session created/gotten")
    return session


def local_session(*, app_name='birgitta_spark_test'):
    """Get a local spark session. Used for recipe tests,
    both running them, and creating fixtures."""
    conf = local_conf_spark()
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
               .appName(app_name)
               .getOrCreate())
    timing.time("spark.local_session created/gotten")
    return session


def is_local():
    return context.get("BIRGITTA_SPARK_SESSION_TYPE") == "LOCAL"


def local_conf_spark():
    """Configure local spark to be fast for recipe tests."""
    # Speed up config for small test data sets
    conf = pyspark.SparkConf().setAll([
        # No parallelism needed in small data
        ('spark.sql.shuffle.partitions', 1)
    ])
    return conf
