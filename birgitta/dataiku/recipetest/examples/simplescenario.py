from birgitta import spark as bspark
from birgitta.dataiku.recipetest import scenariotest
from birgitta.schema.spark import to_spark
from dataiku.scenario import Scenario


spark_session = bspark.session()

scenario = Scenario()
src_project_key = 'TRIBUNE'
src_recipe_key = 'compute_bsparkbookexampleresult'
testbench_project_key = 'TRIBUNE_TESTBENCH'
spark_schema = to_spark([{'name': 'letter', 'type': 'string'},
                         {'name': 'number', 'type': 'bigint'}])

test_params = {
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

scenariotest.test_recipe(spark_session,
                         scenario,
                         src_project_key,
                         src_recipe_key,
                         testbench_project_key,
                         test_params)
