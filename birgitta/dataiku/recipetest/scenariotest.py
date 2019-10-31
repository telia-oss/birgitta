import dataiku
from birgitta import context
from birgitta.dataiku.dataset import manage as dataset_manage
from birgitta.dataiku.dataset.manage import schema
from birgitta.dataiku.recipe import manage as recipe_manage
from birgitta.recipetest import validate


def test_recipe(spark_session,
                scenario,
                src_project_key,
                src_recipe_key,
                testbench_project_key,
                test_params):
    # Trigger dataiku, not parquet
    context.set("BIRGITTA_DATASET_STORAGE", "DATAIKU")
    # Trigger dataiku, not parquet
    context.set("BIRGITTA_S3_BUCKET", "birgitta_s3_bucket")
    print('####################################################')
    print('Test recipe: %s (in project %s)' % (src_recipe_key,
                                               src_project_key))
    if src_project_key == testbench_project_key:
        raise ValueError('Cannot clone recipe to same project as src project')

    print('Clone dataset schemas')
    schemas = test_params['schemas']
    client = dataiku.api_client()
    cloned_input_datasets = schemas['inputs'].keys()
    cloned_input_datasets = clone_schemas(client,
                                          src_project_key,
                                          testbench_project_key,
                                          cloned_input_datasets,
                                          'Inline')
    cloned_output_datasets = schemas['outputs'].keys()
    cloned_output_datasets = clone_schemas(client,
                                           src_project_key,
                                           testbench_project_key,
                                           cloned_output_datasets,
                                           'HDFS')
    expected_output_datasets = create_expected_output_schemas(
        client,
        src_project_key,
        testbench_project_key,
        cloned_output_datasets
    )
    print('Clone recipe')
    recipe_manage.clone(client,
                        src_project_key,
                        src_recipe_key,
                        testbench_project_key,
                        test_name(src_recipe_key),
                        cloned_input_datasets,
                        cloned_output_datasets)

    test_cases = test_params['test_cases']
    for test_case in test_cases:
        print('Setup test case: ' + test_case['name'])
        print('Empty and fill datasets with fixtures')
        empty_and_fill_datasets(testbench_project_key,
                                cloned_input_datasets,
                                schemas['inputs'],
                                test_case['inputs'])
        empty_and_fill_datasets(testbench_project_key,
                                cloned_output_datasets,
                                schemas['outputs'],
                                False)  # empty dataset
        empty_and_fill_datasets(testbench_project_key,
                                expected_output_datasets,
                                expected_params(schemas['outputs']),
                                expected_params(test_case['outputs']))
        print('Run recipe')
        testbench_output_dataset_key = test_params['principal_output_dataset']
        scenario.build_dataset(dataset_name=testbench_output_dataset_key,
                               project_key=testbench_project_key)
        print('Validate output')
        for dataset_name in test_case['outputs']:
            print('Validate output dataset: %s' % (dataset_name))
            validate.datasets(spark_session,
                              dataset_name,
                              expected_name(dataset_name),
                              testbench_project_key)
            print('Successfully validated output dataset: %s' % (dataset_name))
    print('Delete testbench recipe TODO')
    print('Delete datasets TODO')
    print('Tests successful')


def test_name(recipe_name):
    return recipe_name + '_test'


def expected_name(dataset_name):
    return dataset_name + '_expected'


def delete_datasets(project, dataset_names):
    for dataset_name in dataset_names:
        dataset_manage.delete_if_exists(project, dataset_name)


def empty_and_fill_datasets(project_key,
                            dataset_names,
                            schemas,
                            row_sets=False):
    for dataset_name in dataset_names:
        rows = row_sets[dataset_name]['rows'] if row_sets else []
        dataset_manage.empty_and_fill(project_key,
                                      dataset_name,
                                      schemas[dataset_name],
                                      rows)


def clone_schemas(client,
                  src_project_key,
                  dst_project_key,
                  dataset_names,
                  output_type):
    datasets = []
    for dataset_name in dataset_names:
        datasets.append(dataset_name)
        schema.clone(client,
                     src_project_key,
                     dst_project_key,
                     dataset_name,
                     dataset_name,
                     output_type)

    return datasets


def create_expected_output_schemas(client,
                                   src_project_key,
                                   testbench_project_key,
                                   dataset_names):
    datasets = []
    for dataset_name in dataset_names:
        datasets.append(expected_name(dataset_name))
        schema.clone(client,
                     src_project_key,
                     testbench_project_key,
                     dataset_name,
                     expected_name(dataset_name),
                     'Inline')

    return datasets


def expected_params(set_params):
    ret = {}
    for dataset_name in set_params.keys():
        ret[expected_name(dataset_name)] = set_params[dataset_name]
    return ret
