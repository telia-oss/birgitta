import pytest
from birgitta.dataframesource.sources.localsource import LocalSource
from birgitta.recipetest.localtest import assertion
from tests.datasets import fixtures_schema


@pytest.fixture()
def dataframe_source(tmpdir):
    return LocalSource(dataset_dir=tmpdir.strpath)


@pytest.fixture()
def fixtures_data():
    return [
        ['a', 1],
        ['b', 2],
        ['c', 3],
    ]


@pytest.fixture()
def fixtures(spark_session, fixtures_data):
    fixtures = spark_session.createDataFrame(fixtures_data, fixtures_schema)
    return fixtures


@pytest.fixture()
def expected(spark_session, fixtures_data):
    expected = spark_session.createDataFrame(fixtures_data, fixtures_schema)
    return expected


@pytest.fixture(scope="function")
def expected_dict(expected):
    return {
        'assert_example': expected
    }


def test_assert_outputs(dataframe_source,
                        expected_dict,
                        fixtures,
                        spark_session):
    dataframe_source.write(fixtures, 'assert_example')
    assertion.assert_outputs(expected_dict, dataframe_source, spark_session)


def test_write_with_schema(dataframe_source,
                           expected_dict,
                           fixtures,
                           spark_session,
                           capsys):
    dataframe_source.write(fixtures.drop('number'), 'assert_example')
    with pytest.raises(AssertionError):
        assertion.assert_outputs(expected_dict,
                                 dataframe_source,
                                 spark_session)
    captured = capsys.readouterr()
    assert captured.out == (
        "\n============== Data Frame Schema: " +
        """assert_example_result (left) ==============

0:letter:StringTy

============== """ +
        "Data Frame Schema: assert_example_expected (right) " +
        """==============

0:letter:StringTy, 1:number:IntegerT

============== Data Frame Schema Diff: assert_example ==============

Not in result: ['1:number:IntegerType']

============== Assertion Error: assert_example ==============

DataFrame are different

DataFrame shape mismatch
[left]:  (3, 1)
[right]: (3, 2)

Result for assert_example (first 20)

+------+
|letter|
+------+
|a     |
|b     |
|c     |
+------+


Expected for assert_example (first 20)

+------+------+
|letter|number|
+------+------+
|a     |1     |
|b     |2     |
|c     |3     |
+------+------+

""")
    assert captured.err == ""
