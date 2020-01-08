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
if 'dataikuapi' in sys.modules:
    del sys.modules['dataikuapi']
if 'dataikuapi.dss.project.DSSProject' in sys.modules:
    del sys.modules['dataikuapi.dss.project.DSSProject']

dataiku_mock = mock.MagicMock()
sys.modules['dataiku'] = dataiku_mock
spark_mock = mock.MagicMock()
sys.modules['dataiku.spark'] = spark_mock
ds_mock = mock.MagicMock()
sys.modules['dataiku.Dataset'] = ds_mock
dataikuapi_mock = mock.MagicMock()
sys.modules['dataikuapi'] = dataikuapi_mock
project_mock = mock.MagicMock()
sys.modules['dataikuapi.dss.project.DSSProject'] = project_mock
project_obj_mock = mock.MagicMock()
project_mock.return_value = project_obj_mock
dapi_dataset_mock = mock.MagicMock()
project_obj_mock.get_dataset.return_value = dapi_dataset_mock
import dataikuapi.dss.project.DSSProject  # noqa l202
dataikuapi.dss.project.DSSProject.return_value = project_obj_mock


from birgitta.dataframesource.sources.dataikusource import DataikuSource  # noqa F401
from birgitta.dataframe import dataframe, dfdiff  # noqa l202
from birgitta.dataiku import schema as dkuschema  # noqa E402
from birgitta.fields import Catalog  # noqa E402
from birgitta.schema.schema import Schema  # noqa E402


def test_write_without_set_schema():
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
    dapi_dataset_mock.set_schema.assert_not_called()


def test_write():
    # dapi_dataset_mock = mock.MagicMock()
    # project_obj_mock.get_dataset.return_value = dapi_dataset_mock
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
                    dataframe_source=dataiku_source)
    dataiku_schema = dkuschema.to_dataiku(schema)
    dapi_dataset_mock.set_schema.assert_called_once_with(dataiku_schema)
    # dso_mock.set_schema.assert_called_once_with(dataiku_schema)
