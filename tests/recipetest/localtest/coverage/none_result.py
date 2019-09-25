from birgitta.recipetest.coverage.transform import prepare


def read_fixture(name):
    fixture_path = "tests/recipetest/localtest/coverage/fixtures/%s.py"
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
