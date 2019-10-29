import pytest
from birgitta import context
from birgitta.dataframesource.sources.localsource import LocalSource
from birgitta.recipe import runner
from examples.organizations.newsltd.projects import tribune
from pyspark.sql.utils import AnalysisException


@pytest.fixture()
def dataframe_source(tmpdir):
    source_params = {'dataset_dir': tmpdir.strpath}
    return LocalSource(source_params)


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
            "old": "sql_context = ",
            "new": "sql_contesdaf@_0=~> = "
        }
    ]
    with pytest.raises(SyntaxError):
        runner.run(tribune,
                   "recipes/compute_filtered_contracts.py",
                   dataframe_source,
                   replacements)
