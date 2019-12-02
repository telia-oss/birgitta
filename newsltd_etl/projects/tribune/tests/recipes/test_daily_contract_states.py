import pytest
from birgitta.recipetest.coverage.report import transform_coverage  # noqa F401
from birgitta.recipetest.localtest import run_case_partial,recipe_path  # noqa F401

from ..fixtures.daily_contract_states import fixture as daily_contract_states
from ..fixtures.date_dim import fixture as date_dim
from ..fixtures.filtered_contracts import fixture as filtered_contracts


@pytest.fixture()
def run_case(run_case_partial):  # noqa F811
    return run_case_partial(
        [date_dim, filtered_contracts],
        [daily_contract_states],
        recipe_path(__file__)
    )


def test_default(run_case):
    run_case("default")


def test_coverage(transform_coverage):  # noqa F811
    print("Validate coverage")