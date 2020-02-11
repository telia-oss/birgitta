import json
import os

import fixtureorg.bookltd
import pytest  # noqa 401
from birgitta.schema.fixtures import json as fx_json


def test_make(tmpdir):
    root_dir = org_root(__file__)
    result_json_file = F"{root_dir}/fixtureorg/bookltd/projects/ignatius/tests/fixtures/generated_json/contract_data/default.json"  # noqa 501
    fx_json.make(fixtureorg.bookltd)
    expected_json_file = F"{root_dir}/expected/contract_data/default.json"  # noqa 501
    assert_json_files_equal(
        expected_json_file,
        result_json_file
    )


def assert_json_files_equal(expected, result):
    expected_json = read_json_file(expected)
    result_json = read_json_file(result)
    assert expected_json == result_json


def read_json_file(file_path):
    with open(file_path) as json_file:
        return json.load(json_file)


def org_root(file_path):
    return os.path.dirname(os.path.realpath(__file__))
