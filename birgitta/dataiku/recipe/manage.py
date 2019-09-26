from dataikuapi import CodeRecipeCreator
from dataikuapi.dss.recipe import DSSRecipe


def delete_if_exists(project, key):
    for recipe_meta in project.list_recipes():
        # pprint(recipe_meta)
        if key == recipe_meta['name']:
            recipe_to_delete = project.get_recipe(key)
            recipe_to_delete.delete()
            break


def clone(client,
          src_project_key,
          src_recipe_key,
          testbench_project_key,
          testbench_recipe_key,
          input_dataset_names,
          output_dataset_names):
    # src_dataset = src_project.get_dataset(src_fixtures_dataset_key)
    testbench_project = client.get_project(testbench_project_key)
    delete_if_exists(testbench_project, testbench_recipe_key)

    src_recipe = DSSRecipe(client, src_project_key, src_recipe_key)
    definition_and_payload = src_recipe.get_definition_and_payload()
    # Assume pyspark recipe, FUTURE support other types
    recipe_type = "pyspark"
    builder = CodeRecipeCreator(testbench_recipe_key,
                                recipe_type,
                                testbench_project)
    # source code
    builder = builder.with_script(definition_and_payload.get_payload())

    # Add inputs and outputs
    for dataset_name in input_dataset_names:
        builder = builder.with_input(dataset_name)
    for dataset_name in output_dataset_names:
        builder = builder.with_output(dataset_name)
    recipe = builder.build()

    return recipe
