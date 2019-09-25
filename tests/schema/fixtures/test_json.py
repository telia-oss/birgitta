import json
import re

import pytest  # noqa 401
from birgitta.schema.fixtures import json as fx_json


def test_make(tmpdir):
    dst_dir = tmpdir.mkdir("json_fixtures")
    result_json_file = F"{dst_dir}/projects/tribune/tests/fixtures/generated_json/filtered_contracts/fx_default.json"  # noqa 501
    fx_json.make('examples.organizations.newsltd', dst_dir)
    repo_dir = repo_root(__file__)
    expected_json_file = F"{repo_dir}/tests/schema/fixtures/expected/filtered_contracts/fx_default.json"  # noqa 501
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


def repo_root(file_path):
    return re.search('(^.*/birgitta)/tests', file_path)[1]
