import pytest
from birgitta import spark
from birgitta.schema.spark import print_df_rows
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


@pytest.fixture(scope="session")
def spark_session():
    return spark.local_session()  # duration: about 3secs


fixtures_schema = StructType([
    StructField('letter', StringType()),
    StructField('number', IntegerType())
])


@pytest.fixture()
def fixtures_data():
    return [
        ['a', 1],
        ['b', 2]
    ]


@pytest.fixture()
def df_print():
    return """[
    {
        'letter':  'a',
        'number':  1
    },
    {
        'letter':  'b',
        'number':  2
    }
]
"""


@pytest.mark.filterwarnings("ignore:numpy.ufunc size changed")
def test_print_df_rows(spark_session,
                       fixtures_data,
                       df_print,
                       capsys):
    df = spark_session.createDataFrame(fixtures_data, fixtures_schema)
    print_df_rows(df)
    captured = capsys.readouterr()
    assert captured.out == df_print
