"""Functionality for running fixture based test cases.
"""
import os
from functools import partial

import pytest
from birgitta import timing
from birgitta.dataframesource.sources.localsource import LocalSource
from birgitta.recipe import runner
from birgitta.recipetest import localtest
from birgitta.recipetest.coverage import report
from birgitta.recipetest.localtest import fixturing, assertion, script_prepend  # noqa F401
from birgitta.recipetest.coverage.report import cov_report, dbg_counts, cov_results  # noqa F401
from birgitta.recipetest.coverage.transform import prepare

__all__ = ['recipe_path', 'run_case_partial']


@pytest.fixture("function")  # noqa F401
def run_case_partial(tmpdir,
                     cov_report,  # noqa F811
                     cov_results,  # noqa F811
                     spark_session,
                     request):
    """Create a run_case partial to enable the running fixture
    based cases, referring only to the fixture name. Example:

    ```
    def run_case(run_case_partial):   # noqa F811
    return run_case_partial([[ds_contracts, fx_contracts]],
                            [[ds_filtered_contracts,
                              fx_filtered_contracts]],
                            recipe_path(__file__))


    def test_default(run_case):
        run_case("default")
    ```

    Args:
        input_datasets (list): A list with an entry of for each input dataset,
        each entry is a list of the corresponding dataset and fixture module.
        expected_datasets (list): A list with an entry of for each output dataset,
        each entry is a list of the corresponding dataset and fixture module.
        recipe_path (str): File path of the spark recipe.

    Returns:
        the partial with its required resources.
    """
    test_case = request.node.name
    return partial(transposed_partial,
                   tmpdir,
                   cov_report,
                   cov_results,
                   test_case,
                   spark_session)


def recipe_path(file_path, prefix="compute_"):
    """Derive the recipe path given the test path.

    Args:
        file_path (str): Path to the test_ file.
        prefix (str): Prefix of the spark recipe, defaults to '_compute'.

    Returns:
        File path of the recipe.
    """
    dir_path = os.path.dirname(os.path.abspath(file_path))
    filename = os.path.basename(file_path).replace("test_", prefix)
    return F"{dir_path}/../../recipes/{filename}" # noqa F501


def transposed_partial(*argv):
    """Utility function to transpose list of partial args"""
    args = list(argv)
    args.insert(0, run_case)
    return partial(*args)


def run_case(tmpdir,
             cov_report_file,
             cov_results,  # noqa F811
             test_case,
             spark_session,
             in_fixtures,
             out_fixtures,
             recipe_path,
             fixture_name):
    """Run a test case.

    Does the following:

    * Reporting and timing.
    * Setup input and result fixtures.
    * Run the spark script in recipe_path.
    * Asserts the outputs.
    * Collects and presents report.

    Returns:
        None
    """
    timing.time("run_case_fn start: %s" % (fixture_name))
    tdir = tmpdir.strpath
    dataframe_source = LocalSource(dataset_dir=tdir)
    fixture_name = test_case[5:]  # Lose 'test_' prefix
    fixturing.write_fixtures(in_fixtures,
                             fixture_name,
                             spark_session,
                             dataframe_source)
    expected_dfs = fixturing.dataframes(out_fixtures,
                                        fixture_name,
                                        spark_session)
    localtest.process_recipe(recipe_path,
                             tdir,
                             dataframe_source,
                             cov_report_file,
                             test_case,
                             spark_session)
    timing.time("runcase_fn run_script done: %s" % (fixture_name))
    assertion.assert_outputs(expected_dfs,
                             dataframe_source,
                             spark_session)
    report.collect(cov_report_file, test_case, cov_results)
    timing.time("run_case_fn end: %s" % (fixture_name))
    timing.print_results(test_case)


def process_recipe(path,
                   tdir,
                   dataframe_source,
                   cov_report_file,
                   test_case,
                   spark_session):
    """Prepare the recipe and trigger the recipe execution.
    """
    with open(path) as f:
        code = f.read()
    code_w_reporting = prepare(code)
    cov_dict = {}
    cov_dict["cov_report_file"] = cov_report_file
    cov_dict["test_case"] = test_case

    globals_dict = {
        "BIRGITTA_SPARK_SESSION_TYPE": "LOCAL",
        "BIRGITTA_TEST_COVERAGE": cov_dict,
        "BIRGITTA_DBG_COUNTS": dbg_counts()
    }
    full_code = script_prepend.code(tdir) + code_w_reporting
    dump_test_recipe(test_case, tdir, full_code)
    timing.time("execute_recipe before exec")
    runner.exec_code(full_code, globals_dict)
    timing.time("execute_recipe after exec")


def dump_test_recipe(test_case, tmpdir, code):
    dump_path = tmpdir + "/" + test_case + ".py"
    print("\nTest recipe python file:\n", repr(dump_path), "\n")
    with open(dump_path, "w") as f:
        f.write(code)
