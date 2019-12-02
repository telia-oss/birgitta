import pytest
from birgitta.recipetest.coverage.report import transform_coverage  # noqa F401
from birgitta.recipetest.localtest import run_case_partial, recipe_path  # noqa F401

from ..fixtures.contract_data import fixture as contract_data
from ..fixtures.contracts import fixture as contracts


@pytest.fixture()
def run_case(run_case_partial):  # noqa F811
    return run_case_partial(
        [contract_data],
        [contracts],
        recipe_path(__file__))


def test_default(run_case):
    run_case("default")


def test_coverage(transform_coverage):  # noqa F811
    print("Validate coverage")
