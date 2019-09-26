import sys

import pytest  # noqa 401
from birgitta.cli import __main__


def test_main(mocker, mock_json_make, tmpdir):
    dst_dir = tmpdir.mkdir("json_fixtures")
    testargs = ['birgitta',
                'json-fixtures',
                'examples.organizations.newsltd',
                str(dst_dir)]

    with mocker.mock_module.patch.object(sys, 'argv', testargs):
        __main__.main()
        __main__.fx_json.make.assert_called_with(
            'examples.organizations.newsltd',
            dst_dir)


@pytest.fixture
def mock_json_make(mocker):
    mocker.patch.object(__main__.fx_json, 'make', autospec=True)
