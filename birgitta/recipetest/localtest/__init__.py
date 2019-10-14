"""Functionality for running fixture based test cases.
"""
import os
from functools import partial

import pytest
from birgitta import glob
from birgitta import timing
from birgitta.recipe import runner
from birgitta.recipetest import localtest
from birgitta.recipetest.coverage import report
from birgitta.recipetest.localtest import fixturing, assertion, script_prepend  # noqa 401
from birgitta.recipetest.coverage.report import cov_report, dbg_counts, cov_results  # noqa 401
from birgitta.recipetest.coverage.transform import prepare

__all__ = ['recipe_path', 'run_case_partial']


@pytest.fixture("function")  # noqa 401
def run_case_partial(tmpdir,
                     cov_report,  # noqa 811
                     cov_results,  # noqa 811
                     spark_session,
                     request):
    """Create a run_case partial to enable the running fixture
    based cases, referring only to the fixture name. Example:

    ```
    def run_case(run_case_partial):   # noqa 811
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
    return F"{dir_path}/../../recipes/{filename}" # noqa 501


def transposed_partial(*argv):
    """Utility function to transpose list of partial args"""
    args = list(argv)
    args.insert(0, run_case)
    return partial(*args)


def run_case(tmpdir,
             cov_report_file,
             cov_results,  # noqa 811
             test_case,
             spark_session,
             in_datasets,
             out_datasets,
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
    # in_fixtures duration: approx 2 secs
    in_fixture_fns = fixturing.obtain_fixture_fns(in_datasets,
                                                  fixture_name)
    # out_fixtures duration: approx 0.05 secs
    out_fixture_fns = fixturing.obtain_fixture_fns(out_datasets,
                                                   fixture_name)
    tdir = tmpdir.strpath
    # process_recipe duration: approx 7.2 secs
    localtest.process_recipe(recipe_path,
                             tdir,
                             cov_report_file,
                             test_case,
                             in_fixture_fns,
                             spark_session)
    timing.time("runcase_fn run_script done: %s" % (fixture_name))
    assertion.assert_outputs(out_fixture_fns,
                             tdir,
                             spark_session)
    report.collect(cov_report_file, test_case, cov_results)
    timing.time("run_case_fn end: %s" % (fixture_name))
    timing.print_results(test_case)


def process_recipe(path,
                   tmpdir,
                   cov_report_file,
                   test_case,
                   in_fixture_fns,
                   spark_session):
    """Prepare the recipe and trigger the recipe execution.
    """
    with open(path) as f:
        code = f.read()
    code_w_reporting = prepare(code)
    dump_test_recipe(test_case, tmpdir, code_w_reporting)
    cov_dict = {}
    cov_dict["cov_report_file"] = cov_report_file
    cov_dict["test_case"] = test_case

    globals_dict = {
        "BIRGITTA_DATASET_STORAGE": glob.get("BIRGITTA_DATASET_STORAGE"),
        "BIRGITTA_TEST_COVERAGE": cov_dict,
        "BIRGITTA_DBG_COUNTS": dbg_counts(),
        "BIRGITTA_FILEBASED_DATASETS": {
            "dataset_dir": tmpdir
        }
    }
    fixturing.write_fixtures(globals_dict,
                             in_fixture_fns,
                             tmpdir,
                             spark_session)
    full_code = script_prepend.code() + code_w_reporting
    timing.time("execute_recipe before exec")
    runner.exec_code(full_code, globals_dict)
    timing.time("execute_recipe after exec")


def dump_test_recipe(test_case, tmpdir, code):
    dump_path = tmpdir + "/" + test_case + ".py"
    print("\nTest recipe python file:\n", repr(dump_path), "\n")
    with open(dump_path, "w") as f:
        f.write(code)
