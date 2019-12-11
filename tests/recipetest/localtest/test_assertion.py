import pytest
from birgitta import spark
from birgitta.dataframesource.sources.localsource import LocalSource
from birgitta.recipetest.localtest import assertion
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


@pytest.fixture(scope="session")
def spark_session():
    return spark.local_session()


@pytest.fixture()
def dataframe_source(tmpdir):
    return LocalSource(dataset_dir=tmpdir.strpath)


fixtures_schema = StructType([
    StructField('letter', StringType()),
    StructField('number', IntegerType())
])


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


def test_assert_outputs_missing_col(dataframe_source,
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
        "============== Data Frame Schema: " +
        """assert_example_result (left) ==============
0:letter:StringTy
============== """ +
        "Data Frame Schema: assert_example_expected (right) " +
        """==============
0:letter:StringTy, 1:number:IntegerT
============== Data Frame Schema Diff: assert_example ==============
Not in result: ['1:number:IntegerType']
""")
    assert captured.err == ""


def test_assert_outputs_missing_rows(dataframe_source,
                                     expected_dict,
                                     fixtures,
                                     spark_session,
                                     capsys):
    dataframe_source.write(fixtures.limit(2), 'assert_example')
    with pytest.raises(AssertionError):
        assertion.assert_outputs(expected_dict,
                                 dataframe_source,
                                 spark_session)
    captured = capsys.readouterr()
    assert captured.out == (
        "============== Data Frame Schema: " +
        """assert_example_result (left) ==============
0:letter:StringTy, 1:number:IntegerT
============== """ +
        "Data Frame Schema: assert_example_expected (right) " +
        """==============
0:letter:StringTy, 1:number:IntegerT
============== Data Frame Schema Diff: assert_example ==============
""")
    assert captured.err == ""
