"""Helpers and imports for pyspark unit tests
"""
import pytest
from birgitta import context
from birgitta import spark
from birgitta.schema.fixtures import values

# We must import these fixtures explicitly to make them available
# in localtest fixtures at run time.
from birgitta.recipetest.coverage.report import cov_report  # noqa 401
from birgitta.recipetest.coverage.report import cov_report_path  # noqa 401
from birgitta.recipetest.coverage.report import cov_results  # noqa 401


@pytest.fixture(scope="session")
def spark_session():
    return spark.local_session()  # duration: about 3secs


context.set("TODAY", values.today())
