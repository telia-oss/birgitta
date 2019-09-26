import pytest
from birgitta.recipetest.coverage.report import transform_coverage  # noqa 401
from birgitta.recipetest.localtest import run_case_partial,recipe_path  # noqa 401
from ...datasets import date_dim as ds_date_dim  # noqa 501
from ...datasets import filtered_contracts as ds_filtered_contracts  # noqa 402
from ...datasets import daily_contract_states as ds_daily_contract_states  # noqa 402
from ..fixtures import date_dim as fx_date_dim  # noqa 501
from ..fixtures import filtered_contracts as fx_filtered_contracts # noqa 402
from ..fixtures import daily_contract_states as fx_daily_contract_states  # noqa 402


@pytest.fixture()
def run_case(run_case_partial):  # noqa 811
    return run_case_partial([
        [ds_date_dim, fx_date_dim],
        [ds_filtered_contracts, fx_filtered_contracts]
    ],
                            [[ds_daily_contract_states,
                              fx_daily_contract_states]],
                            recipe_path(__file__))


def test_default(run_case):
    run_case("default")


def test_coverage(transform_coverage):  # noqa 811
    print("Validate coverage")
