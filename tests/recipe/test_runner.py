import pytest
from birgitta import context
from birgitta.dataframesource.sources.localsource import LocalSource
from birgitta.recipe import runner
from examples.organizations.newsltd.projects import tribune
from pyspark.sql.utils import AnalysisException


@pytest.fixture()
def dataframe_source(tmpdir):
    return LocalSource(dataset_dir=tmpdir.strpath)


def test_run(dataframe_source):
    context.reset()
    with pytest.raises(AnalysisException):
        runner.run(tribune,
                   "recipes/compute_filtered_contracts.py",
                   dataframe_source)


def test_syntax_error(dataframe_source):
    context.reset()
    replacements = [
        {
            "old": "spark_session = ",
            "new": "spark_sesssdaf@_0=~> = "
        }
    ]
    with pytest.raises(SyntaxError):
        runner.run(tribune,
                   "recipes/compute_filtered_contracts.py",
                   dataframe_source,
                   replacements)
