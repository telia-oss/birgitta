import os

from birgitta.recipetest.coverage.transform import prepare


def read_fixture(name):
    file_path = os.path.dirname(os.path.abspath(__file__))
    fixture_path = F"{file_path}/fixtures/%s.py" # noqa 501
    with open(fixture_path % (name)) as f:
        return f.read()


def test_some_transforms():
    fixture = read_fixture("default_in")
    expected = read_fixture("default_expected")
    result = prepare(fixture)
    assert result == expected


def test_no_transforms():
    fixture = read_fixture("none_in")
    expected = read_fixture("none_expected")
    result = prepare(fixture)
    assert result == expected
