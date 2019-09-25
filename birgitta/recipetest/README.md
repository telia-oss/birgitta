# Recipetest
A module for unit testing recipes.

## Localtest: Unit testing spark recipes locally

### Localtest flow

The [localtest](localtest/__init__.py) does the following:

1. Produce input and expected output fixtures
   1. Produce data frames with the fixture definitions for each input and output data set
   2. Write data frames to file or memory
2. Run py spark recipe
   1. Collect coverage and timing
3. Compare producted outputs to expected outputs
4. Report

## Scenariotest: Dataiku unit testing of recipes, using schenarios

Example usage can be seen in the text case [simplescenario.py](examples/simplescenario.py).

It will send one of more test cases to a duplicate of a DSS recipe. For now, we only have support for [pyspark recipes](https://doc.dataiku.com/dss/latest/code_recipes/pyspark.html), but implementing support for other recipe types would be fairly easy.

The intention is to unit test a recipetest, especially to test specific aspects of the logic in a recipe.

### Scenariotest flow

The [scenariotest](../dataiku/recipetest/scenariotest.py) does the following:

1. Receive input params:
   1. source project and recipe names
   2. destination testbench project name
   3. input and output schemas
   4. fixture and expected data
2. Clone the data set schemas in the testbench project
3. Clone the recipe in the testbench project
4. For each test case
   1. Empty the input data sets
   2. Insert fixtures in the input data sets
   3. Run the scenario
   4. Compare the outputs to the expected outputs

### Adding a recipe test to a scenario in your project

#### Create a python scenario in your DSS project

Let's assume your project is called `BAR`. Create a new project called `BAR_TEST`.

1. Create a custom python scenario called `recipe_test_foo`, where `foo` is the name of the recipe you want to test.
2. Copy the [simplescenario.py](birgitta/recipetest/examples/simplescenario.py) code to your scenario.
3. Run your test ;)
