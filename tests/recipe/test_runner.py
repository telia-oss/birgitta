import pytest
from birgitta import context
from birgitta.dataframesource.sources.localsource import LocalSource
from birgitta.recipe import runner
from newsltd_etl.projects import tribune
from pyspark.sql.utils import AnalysisException


@pytest.fixture()
def dataframe_source(tmpdir):
    return LocalSource(dataset_dir=tmpdir.strpath)


def test_run(dataframe_source):
    context.reset()
    with pytest.raises(AnalysisException):
        runner.run(tribune,
                   "recipes/compute_filtered_contracts.py",
                   dataframe_source=dataframe_source)


def test_run_no_source():
    context.reset()
    with pytest.raises(AttributeError) as e_info:
        runner.run(tribune,
                   "recipes/compute_filtered_contracts.py")
    assert str(e_info.value) == "'NoneType' object has no attribute 'load'"


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
                   dataframe_source=dataframe_source,
                   replacements=replacements)


def test_run_and_exit(dataframe_source):
    context.reset()
    with pytest.raises(SystemExit) as e_info:
        recipe = "recipes/compute_noop.py"
        runner.run_and_exit(tribune,
                            recipe,
                            dataframe_source=dataframe_source)
    assert str(e_info.value) == '0'
