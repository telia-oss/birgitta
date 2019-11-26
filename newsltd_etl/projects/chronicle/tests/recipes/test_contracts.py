import pytest
from birgitta.recipetest.coverage.report import transform_coverage  # noqa 401
from birgitta.recipetest.localtest import run_case_partial,recipe_path  # noqa 401
from ...datasets import contract_data as ds_contract_data  # noqa 402
from ...datasets import contracts as ds_contracts  # noqa 402
from ..fixtures import contract_data as fx_contract_data  # noqa 402
from ..fixtures import contracts as fx_contracts  # noqa 402


@pytest.fixture()
def run_case(run_case_partial):  # noqa 811
    return run_case_partial(
        [[ds_contract_data, fx_contract_data]],
        [[ds_contracts, fx_contracts]],
        recipe_path(__file__))


def test_default(run_case):
    run_case("default")


def test_coverage(transform_coverage):  # noqa 811
    print("Validate coverage")
