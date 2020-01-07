import sys
from unittest.mock import MagicMock, patch  # noqa F401

import mock
import pytest  # noqa F401

# TODO: Simplify the mocking of private (unavailable) dataiku lib.
# Current mocking is ugly and complex.
if 'dataiku.Dataset' in sys.modules:
    del sys.modules['dataiku.Dataset']
if 'dataiku' in sys.modules:
    del sys.modules['dataiku']
if 'dataiku.Dataset' in sys.modules:
    del sys.modules['dataiku.Dataset']


dataiku_mock = mock.MagicMock()
sys.modules['dataiku'] = dataiku_mock
spark_mock = mock.MagicMock()
sys.modules['dataiku.spark'] = spark_mock
dso_mock = mock.MagicMock()
ds_mock = mock.MagicMock()
sys.modules['dataiku.Dataset'] = ds_mock
ds_mock.return_value = dso_mock
from birgitta.dataframesource.sources.dataikusource import DataikuSource  # noqa F401
from birgitta.dataframe import dataframe, dfdiff  # noqa l202
from birgitta.dataiku import schema as dkuschema  # noqa E402
from birgitta.fields import Catalog  # noqa E402
from birgitta.schema.schema import Schema  # noqa E402
from dataiku import Dataset  # noqa E402

Dataset.return_value = dso_mock


def test_write():
    dataiku_source = DataikuSource()
    dataset_name = "fixtures"
    s3_dir = "s3://birgittatestbucket/sourcetests"
    fixtures_mock = MagicMock()
    catalog = Catalog()
    catalog.add_field('fooint', description='Foo int', example=39)
    schema = Schema([['fooint', 'bigint']], catalog)
    import dataiku
    ds = dataiku.Dataset
    ds.return_value = dso_mock

    dataframe.write(fixtures_mock,
                    dataset_name,
                    prefix=s3_dir,
                    schema=schema,
                    skip_cast=True,
                    dataframe_source=dataiku_source)
    dataiku_schema = dkuschema.to_dataiku(schema)
    dso_mock.set_schema.assert_called_once_with(dataiku_schema)


@mock.patch('dataiku.Dataset')
def test_write_without_set_schema(Dataset):
    dso_mock = mock.MagicMock()  # Reset calls from test_write()
    Dataset.return_value = dso_mock
    dataiku_source = DataikuSource()
    dataset_name = "fixtures"
    s3_dir = "s3://birgittatestbucket/sourcetests"
    fixtures_mock = MagicMock()
    catalog = Catalog()
    catalog.add_field('fooint', description='Foo int', example=39)
    schema = Schema([['fooint', 'bigint']], catalog)
    dataframe.write(fixtures_mock,
                    dataset_name,
                    prefix=s3_dir,
                    schema=schema,
                    skip_cast=True,
                    set_schema=False,
                    dataframe_source=dataiku_source)
    dso_mock.set_schema.assert_not_called()
