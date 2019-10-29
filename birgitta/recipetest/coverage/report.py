"""Evaluate coverage of all transformations in notebook,
ensuring that all of them have handled at least one record, to
promote test coverage of all logic.
"""

import json
import os

import pytest
from birgitta import context
from birgitta import timing

__all__ = [
    'log_transform',
    'collect',
    'cov_report_path',
    'cov_report',
    'cov_results',
    'dbg_counts',
    'transform_coverage'
]


@pytest.fixture(scope="function")
def transform_coverage(request, cov_report_path, cov_results):
    """transform_coverage fixture enables the verification of
    complete transform coverage accumulated for all test cases
    for a recipe. transform_coverage test case is added this way:

    def test_coverage(transform_coverage):  # noqa 811
        print("Validate coverage")

    No specification is needed.
    """
    assign_counts = {}
    raw_report = cov_results['raw']
    for test_case in raw_report.keys():
        get_assign_counts(raw_report[test_case], assign_counts)
    print("\nData frame assign counts:\n")
    assign_counts_print(assign_counts)

    for assign_key in assign_counts.keys():
        record_count = assign_counts[assign_key]["record_count"]
        assert record_count > 0, "Assign has zero count: " + assign_key


def log_transform(var, line_no, line_str):
    """Helper function to log a transformation.
    localtest will insert log_transform() calls after assignments.
    The call contains the line_no and the assignement code.
    The purpose is to log the transformation, including the
    dataframe count if it is a dataframe. The purpose is to
    Verify that more than zero rows pass through each transform.

    Args:
        var (str): The variable being logged
        line_no (int): The line number of the original code line
        line_str (str): The original code line
    """
    report_file = context.get("BIRGITTA_TEST_COVERAGE")["cov_report_file"]
    test_case = context.get("BIRGITTA_TEST_COVERAGE")["test_case"]
    type_name = type(var).__name__
    metrics = {"var_type": type_name}
    if type_name == "DataFrame":
        metrics["count"] = var.count()
    log_entry(test_case, line_no, line_str, report_file, metrics)


def collect(cov_report, test_case, cov_results):
    """Collect all transform reports for test_case and
    print case report.

    Args:
        cov_report (str): report file
        test_case (str): test case being collected
        cov_results (dict): dict holding the results format
        for all test cases. Results for test_case are added.
    """
    line_executions = []
    with open(cov_report) as f:
        report_lines = f.readlines()
        for report_line in report_lines:
            line_executions.append(json.loads(report_line))
        cov_results["raw"][test_case] = line_executions
    print_case_report(test_case, line_executions)


@pytest.fixture(scope="function")
def cov_report(cov_report_path):
    """Ensure coverage report path is setup when running tests."""
    if not os.path.exists(cov_report_path):
        cov_report_path.write("")
    return cov_report_path.strpath


@pytest.fixture(scope="function")
def cov_report_path(tmpdir):
    """Force coverage.log to be present when running tests."""
    return tmpdir.mkdir("coverage").join("coverage.log")


@pytest.fixture("module")  # noqa 401
def cov_results():
    """ Fixture holding transform coverage results from all test cases
    """
    return {
        "raw": {}
    }


def dbg_counts():
    """Check if env var BIRGITTA_DBG_COUNTS is set.
    If true, print out transformation counts when running test."""
    return os.environ.get('BIRGITTA_DBG_COUNTS') is not None


def log_entry(test_case, line_no, line_str, report_file, metrics):
    """Log a report entry for line of the recipe.
    """
    timing.time(line_str)
    if dbg_counts() and (metrics['var_type'] == 'DataFrame'):
        print("l:", line_no, repr(line_str), "count:", metrics['count'])
    with open(report_file, 'a') as f:
        json_dict = {
            "test_case": test_case,
            "line_no": line_no,
            "line_str": line_str,
            "metrics": metrics,
        }
        f.write(json.dumps(json_dict))
        f.write("\n")


def print_case_report(test_case, line_executions):
    """Print count report for the current test_case
    """
    print("\nData frame assign counts for test case " + test_case + ":\n")
    assign_counts = get_assign_counts(line_executions)
    assign_counts_print(assign_counts)


def assign_counts_print(assign_counts):
    """Print assign counts for each line.
    """
    for assign_key in assign_counts:
        assign_count = assign_counts[assign_key]
        print(assign_count["record_count"], "records,",
              assign_count["executions"], "executions,",
              "line:", assign_key)
    print("\n")


def get_assign_counts(line_executions, assign_counts={}):
    """Gather the assign counts for each line.
    """
    for line_execution in line_executions:
        metrics = line_execution['metrics']
        if metrics['var_type'] != "DataFrame":
            continue
        assign_key = "%d:%s" % (line_execution['line_no'],
                                line_execution['line_str'])
        if assign_key not in assign_counts:
            assign_counts[assign_key] = {
                "executions": 0,
                "record_count": 0
            }
        assign_counts[assign_key]['executions'] += 1
        assign_counts[assign_key]['record_count'] += metrics['count']
    return assign_counts
