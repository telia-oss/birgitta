"""Module for test case fixturing functions.
"""
from birgitta import timing


__all__ = ['dataframes', 'write_fixtures']


def dataframes(fixtures, variant_name, spark_session):
    """Makes dataframes from fixtures.

    Args:
        fixtures (dict): Dict of fixtures
        variant_name (str): Name of fixture variant
        spark_session (SparkSesssion): Spark session used to create fixtures
        dataframe_source (DataframeSource): The source to write to, e.g. S3

    Returns:
       A dict of `dataset name => dataframe` pairs
    """
    # FUTURE: Try writing directly to parquet using pyarrow instead. Could be faster. # noqa 501
    ret = {}
    for fixture in fixtures:
        ret[fixture.dataset.name] = fixture.df(variant_name,
                                               spark_session)
    return ret


def write_fixtures(fixtures, variant_name, spark_session, dataframe_source):
    """Write fixtures to storage.

    Args:
        fixtures (dict): Dict of fixtures
        variant_name (str): Name of fixture variant
        spark_session (SparkSession): Spark session used to create fixtures
        dataframe_source (DataframeSource): The source to write to, e.g. FS
    """
    timing.time("write_fixtures start")
    dfs = dataframes(fixtures, variant_name, spark_session)
    for ds_name in dfs.keys():
        dataframe_source.write(dfs[ds_name], ds_name)
    timing.time("write_fixtures end")
