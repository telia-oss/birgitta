import sys

import mock
import pytest
from pyspark.sql import SparkSession


# Mock dataiku sys libs, not available in pip
subtract_df_mock = mock.MagicMock()
subtract_df_mock.count.return_value = 0
df_mock = mock.MagicMock()
df_mock.count.return_value = 0
df_mock.subtract.return_value = subtract_df_mock
spark_mock = mock.MagicMock()
spark_mock.get_dataframe.return_value = df_mock
sys.modules['dataiku.spark'] = spark_mock
dku_mock = mock.MagicMock()
dku_mock.spark = spark_mock
sys.modules['dataiku'] = dku_mock
sys.modules['dataiku.scenario'] = mock.MagicMock()
sys.modules['dataikuapi'] = mock.MagicMock()
sys.modules['dataikuapi.dss'] = mock.MagicMock()
sys.modules['dataikuapi.dss.recipe'] = mock.MagicMock()

import dataiku  # noqa E402
import dataiku.spark as dkuspark  # noqa E402
from birgitta.dataframesource import contextsource  # noqa E402
from birgitta.dataframesource.sources.dataikusource import DataikuSource  # noqa E402
from birgitta.dataiku.recipetest import scenariotest  # noqa E402
from birgitta.schema.spark import simple_to_spark  # noqa E402


@pytest.fixture()
def spark_session(mocker):
    return mocker.MagicMock(SparkSession)


@pytest.fixture()
def scenario():
    return mock.MagicMock()


@pytest.fixture()
def test_params():
    spark_schema = simple_to_spark([['letter', 'string'],
                                    ['number', 'bigint']])

    return {
        'principal_output_dataset': 'exampledatasetoutput',
        'schemas': {
            'inputs': {
                'exampledataset': spark_schema,
            },
            'outputs': {
                'exampledatasetoutput': spark_schema
            }
        },
        'test_cases': [
            {
                'name': 'correct_count',
                'inputs': {
                    'exampledataset': {
                        'rows': [['a', 1], ['b', 2], ['c', 3], ['d', 4]]
                    }
                },
                'outputs': {
                    'exampledatasetoutput': {
                        'rows': [['a', 1], ['b', 2]]
                    }
                }
            }
        ]
    }


@pytest.fixture()
def src_project_key():
    return 'EXAMPLEPROJ'


@pytest.fixture()
def src_recipe_key():
    return 'compute_exampledatasetoutput'


@pytest.fixture()
def testbench_project_key():
    return 'EXAMPLEPROJ_TESTBENCH'


def test_no_assert(spark_session,
                   scenario,
                   test_params,
                   src_project_key,
                   src_recipe_key,
                   testbench_project_key,
                   tmpdir):
    contextsource.set(DataikuSource())
    scenariotest.test_recipe(spark_session,
                             scenario,
                             src_project_key,
                             src_recipe_key,
                             testbench_project_key,
                             test_params)


def test_simplescenario():
    from birgitta import recipetest  # noqa F401
    # with pytest.raises(KeyError):
    from birgitta.dataiku.recipetest.examples import simplescenario  # noqa F401
