import sys
from unittest.mock import MagicMock, patch  # noqa F401


import mock
import pytest  # noqa F401

# # TODO: Simplify the mocking of private (unavailable) dataiku lib.
# # Current mocking is ugly and complex.
# if 'dataiku.Dataset' in sys.modules:
#     del sys.modules['dataiku.Dataset']
# if 'dataiku' in sys.modules:
#     del sys.modules['dataiku']
# if 'dataiku.Dataset' in sys.modules:
#     del sys.modules['dataiku.Dataset']
# if 'dataikuapi' in sys.modules:
#     del sys.modules['dataikuapi']
# # if 'dataikuapi.dss.project.DSSProject' in sys.modules:
# #     del sys.modules['dataikuapi.dss.project.DSSProject']

# dataiku_mock = mock.MagicMock()
# sys.modules['dataiku'] = dataiku_mock
# spark_mock = mock.MagicMock()
# sys.modules['dataiku.spark'] = spark_mock
# ds_mock = mock.MagicMock()
# sys.modules['dataiku.Dataset'] = ds_mock
# dataikuapi_mock = mock.MagicMock()
# sys.modules['dataikuapi'] = dataikuapi_mock
# project_mock = mock.MagicMock()
# # sys.modules['dataikuapi.dss.project.DSSProject'] = project_mock
# # project_obj_mock = mock.MagicMock()
# # project_mock.return_value = project_obj_mock
# # dapi_dataset_mock = mock.MagicMock()
# # project_obj_mock.get_dataset.return_value = dapi_dataset_mock
# # import dataikuapi.dss.project.DSSProject  # noqa l202
# # dataikuapi.dss.project.DSSProject.return_value = project_obj_mock


class DataikuMock(object):
    dss_settings = 'dummy'


@mock.patch("birgitta.dataframesource.sources", mock.Mock())
def test_is_current_platform():
    sys.modules['dataiku'] = DataikuMock()
    from birgitta.dataiku import platform as dkuplatform  # noqa E402
    ret = dkuplatform.is_current_platform()
    print("ret:", repr(ret))
    assert ret
    if 'dataiku' in sys.modules:
        del sys.modules['dataiku']
