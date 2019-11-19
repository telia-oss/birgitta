Recipe, in this context, is a piece of pyspark code that is execute as a Dataiku Recipe. 

Recipe development describes the process of going all the way from an experimental recipe to a production recipe.

The purpose is to achieve stable and solid production recipes while still enabling experimentation, in line with DataOps thinking.

ETL repository is your organizations git repository for storing recipes, schemas, fixtures, tests and shared functions. It's role will be gradually explained below.

Recipe development
How to make a recipe production ready?

Experiments vs production
A recipe often starts with a notebook experiment to create some new data set from one or other two datasets. As the output data set starts to resemble what we need, we want to deploy it to be executed periodically on our production system.

Production-ready
To make a recipe ready for production we must:

Factor our common functionality to git modules, e.g. like the periods.group_by_day() function.
Add schemas to git.
Verify the schema of input and output data sets, using dataframe.write() and read().
Submit the recipe to git.
Add recipe unit tests with fixtures, so the it's correctness is tested.
Override the dataiku recipe code by invoking the git version of the recipe and exit.
Moving from experiment to production-ready
Create a branch
The first step is to create a branch. If your recipe is called `compute_customer_kpis`, then your branch should be called etl/recipe-customer-kpis. Checkout the branch, or start working on it directly in your web browser on your organizations repository. For the purpose of demonstation we will use https://github.com/telia-oss/birgitta-example-etl/tree/master/newsltd_etl as the folder of our organization project.

Factor out common functionality to git modules
To improve code quality and readability it is often useful to factor out common functionality to functions or classes, e.g. like the periods.group_by_day() function. The modules should be added under /libs/ in your repo organization folder, in our case https://github.com/telia-oss/birgitta-example-etl/tree/master/newsltd_etl. All code should be covered by tests, added under `/libs/tests. Typically you factor out code to improve readability or share functionality between recipes.

When the code is somehow ready to run, add a commit. This does not have to be a final commit, as Dataiku can use code from unmerged (unfinished) branches.

Import your master branch of the project
In Dataiku, we are in the TRIBUNE project. Go to project → libraries:

Data & Analytics NO > Dataiku Pyspark Recipe Development Process > image2019-11-14_17-12-3.png

There, do Import from Git:

Data & Analytics NO > Dataiku Pyspark Recipe Development Process > image2019-11-14_17-12-46.png

We will use these settings:

Repository: https://github.com/telia-oss/birgitta-example-etl/
Checkout: master
Path in repository: newsltd_etl
Target path: newsltd_etl
Data & Analytics NO > Dataiku Pyspark Recipe Development Process > image2019-11-19_14-27-39.png



We reference a sub path in the repository, since, the etl (or so-called organization) module is the sub-folder newsltd_etl.

Import your branch to the project, overriding master branch with your development branch
Follow the same steps above, but this time the checkout field should not be master, but rather recipe-customer-kpis.

Then modify extenal-libraries.json so that the recipe-customer-kpi branch overrides code in the master branch, since it loads first:

Data & Analytics NO > Dataiku Pyspark Recipe Development Process > image2019-11-19_14-30-56.png

Update from git




Whenever you commit new code to your branch, press Update from Git on the library in the Library editor list:

Data & Analytics NO > Dataiku Pyspark Recipe Development Process > image2019-11-14_17-21-7.png

If you are in notebook mode, you also need to switch the kernel twice (back and forth) to force the reconstruction of the root folder structure for your notebook, to get the new updates. Hopefully, in the future, Dataiku will provide a more elegant solution to get updates from git, directly from the notebook.

Extracting code for refactoring
Now, let's say you have a function group_by_day() which you want to reuse across recipes, you can then add it to your github library and press Update from Git, and import it from the git based module, while removing it from your recipe code.

In our recipe, from our git repo we can invoke the function by importing the periods module:

from newsltd_etl.libs import periods

group_keys = ['prod_code', 'shop_code', 'channel']
    from_date_field = 'from_date'
    to_date_field = 'to_date'
    grouped = periods.group_by_day(fixtures,
                                   group_keys=group_keys,
                                   from_date_field=from_date_field,
                                   to_date_field=to_date_field,
                                   join_partitions=1,
                                   count_partitions=1,
                                   min_cutoff_date=min_date,
                                   max_cutoff_date=max_date,
                                   spark_session=spark_session,
                                   drop_date_fields=True)
Note whatever version exists in the recipe-customer-kpi branch will be loaded before the master branch, allowing us to make continuous changes to the git version while testing it in the recipe. Note that for each change we must Update from Git button on the branch version of the repo (newsltd_etl_recipe-customer-kpi). 

Adding schemas
Adding output schema
Adding schemas is a tool for robustness in your recipes, and is a precondition for the birgitta unit tests. The first step is to define the output schema, which is passed to dataframe.write():

Initially, in dataiku recipes a data set is written with dkuspark.write_with_schema(). This must be change to use birgitta.dataframe.write():

#### Old dataiku version:
# dku_contracts = dataiku.Dataset("contracts")
# dkuspark.write_with_schema(dku_contacts, to_output_df)
#
#### Now changed to:
from newsltd_etl.projects.chronicle.datasets.contracts import dataset as ds_contracts
dataframe.write(to_output_df, "contracts", schema=ds_contracts.schema)
For this to work we first need to add that schema to github. The file must look like this:

from birgitta.schema.schema import Schema
from newsltd_etl.shared.schema.catalog.tribune_chronicle import catalog

fields = [
    ['customer_id', 'bigint'],
    ['phone', 'string'],
    ['chronicle_account_id', 'bigint'],
    ['group_account_id', 'bigint'],
    ['start_date', 'date'],
    ['end_date', 'date'],
    ['priceplan_code', 'string'],
    ['current_flag', 'bigint'],
    ['client_status_code', 'bigint']
]

schema = Schema(fields, catalog)
The catalog defines all the fields we have in our domain. New entries with example values must be added for all fields we have in schema. This serves both as documentation and for creating test fixtures. Also, it serves as a data field catalog, helping us to organize our field names.

When the schema and the catalog entries have been added to our new git branch, we press Update from git again, and can start using them. We can now execute the dataframe.write() line with the added protection of a schema.

This can be tested in a notebook or in Dataiku recipe mode without adding input schemas.

The actual schema itself can be derived by using the utility function birgitta.schema.spark.from_spark_df() in a notebook:

Simple way of deriving schema
For this to work we first need to add that schema to github. The file must look like this:

from birgitta.schema import spark as schemaspark
# to_output_df is the schema we want to get the schema from:
schemaspark.from_spark_df(to_output_df)
# Output:
# fields = [
#     ['customer_id', 'bigint'],
#     ['phone', 'string'],
#     ['chronicle_account_id', 'bigint'],
#     ['group_account_id', 'bigint'],
#     ['start_date', 'date'],
#     ['end_date', 'date'],
#     ['priceplan_code', 'string'],
#     ['current_flag', 'bigint'],
#     ['client_status_code', 'bigint']
# ]
Adding input schemas
For adding input schemas, we follow the same methodology as for adding output schema. Add schemas for every input dataset. Currently, schemas are not enforced on dataframe.read() but this will be added in the future to birgitta.dataframe.

You must replace all your dkuspark.get_dataframe() calls with birgitta's dataframe.read() commands, in order for your units test to be able to run locally:

#### Old dataiku version:
# contract_data_ds = dataiku.Dataset("contract_data")
# contract_data_df = dkuspark.get_dataframe(sqlContext, contract_data_ds)
#### Now changed to:
from newsltd_etl.projects.chronicle.datasets.contract_data import dataset as ds_contract_data
contract_data_df = dataframe.get(spark_session, ds_contract_data.name)
Writing tests and committing recipe
When a recipe seems stable, copy the code into a python file on your local machine where you have a clone of the analytics-no repo. If the recipe is in the dataiku project chronicle, and is called compute_contracts.py then the code should go in the path `newsltd_etl/projects/chronicle/recipes/compute_contracts.py`.

Writing the test
This is an example of how the test should look, and what its path should be: newsltd_etl/projects/chronicle/tests/recipes/test_contracts.py.

Running the test
If you set up your reposity like birgitta-example-etl. You can run the test with make test from the root of the repository. You can also run the test with:

pytest newsltd_etl/projects/chronicle/tests/recipes/test_contracts.py

To set up the repository, see the install instructions in birgitta-example-etl. It is show how to add automatic testing with travis on Pull Request merge.

Note that on Dataiku, the pip modules your recipes depend on must be installed in your Dataiku Code Environment. Dataiku unfortunately does not install dependent module when adding a git repository as a python module. 

When the test runs green, you can commit the recipe and the tests to your branch.

In the future we might add helper functions for execution github recipe tests directly from Dataiku, for convenience to avoid having to set it up locally.

Git override recipe




When a stable and tested version of the recipe has been added to git, the recipe in Dataiku should be short cut with a git runner. This way the code that is run is the tested and versioned code in github.

Note that the old code is exactly the same as the code in github, including the usage of schemas birgitta.dataframe.read() and write().

The old code can be left below for convenience, if you want to go back and hack around on it later. Some people might prefer to just delete the old code. 

from birgitta.dataframesource.sources.dataikusource import DataikuSource
from birgitta.recipe import runner
from examples.organizations.newsltd.projects import tribune
runner.run_and_exit(tribune, "recipes/compute_filtered_contracts.py", DataikuSource())

### The recipe below stays the same, but code is never run ###

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
from birgitta import spark as bspark
from birgitta.dataframe import dataframe
from pyspark.sql import functions as F
from examples.organizations.newsltd.projects.tribune.datasets.filtered_contracts import dataset as ds_filtered_contracts # noqa 501
from examples.organizations.newsltd.projects.tribune.datasets.contracts import dataset as ds_contracts # noqa 501

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#  Get or create sparkcontext and set up sqlcontext
spark_session = bspark.session()
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
contracts = dataframe.get(spark_session,
                     ds_contracts.name,
                     cast_binary_to_str=True)

...

With the new code in git, the Dataiku project can now be more safely deployed to production.

Git based updates of Dataiku production recipes without redeploy
An added benefit is that any new changes merged into git will be automatically picked up by your Dataiku production recipe, when you press Update from git in the Production project library editor. This is possible because the code being run comes from git and not from the recipe interface.





Merging your PR into production
If you base your etl repository on birgitta-example-etl, you can easily add support for automatic unit testing of recipes on Pull Request merge. When your Pull Request tests run green, you can somewhat more safely merge to master.

After merging to master, and before deploying your Dataiku project to Automation (production), remember to remove the imported library based on the recipe-customer-kpi branch, so that only the master branch library remains.

Iterative improvements and refinement
If you want to come back to your recipe and do changes or improvements, you simply comment out the runner.run_and_exit() lines and continue to work in your code. When you have the improvements ready, you add them to a new branch and run your tests locally. When running green, you create a Pull Request. After the changes have been merged into master you can reenable runner.run_and_exit(), but make sure you Update from git on your etl git library in your project library editor. On automation you just need to Update from git to get the latest changes.

Advanced testing
Some advanced testing features to mention are multiple fixtures, json fixtures and transformation coverage testing.

Multiple fixtures
If needed, you can cover more logic in our recipes with multiple sets of fixtures:

def fx_default(spark):
    return fixtures.df(spark, schema)


def fx_brand_code_44(spark):
    row_confs = [
        {"brand_code": {"example": {"static": 44}}}
    ]
    return fixtures.df(spark, schema, row_confs)
You need to have the same fixtures available for all input and output fixtures for a recipe test. You then explicitly invoke both variations of the test:

def test_default(run_case):
    run_case("default")


def test_brand_code_44(run_case):
    run_case("brand_code_44")
JSON fixtures
For debugging and visualization, birgitta supports generating json examples of your fixtures. In birgitta-example-etl this is invoked by make json_fixtures which in turn calls a python script doing:

import newsltd_etl
from birgitta.schema.fixtures import json as fx_json


fx_json.make(newsltd_etl)
Here is an example JSON output of the brand_code_44 fixture for contracts.

Transformation coverage
See https://github.com/telia-oss/birgitta#transformation-coverage-testing.
