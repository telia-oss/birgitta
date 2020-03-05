import inspect
import os

import pytest
from birgitta.recipe import diff, find
from newsltd_etl import projects


@pytest.fixture()
def projects_path():
    return os.path.dirname(inspect.getfile(projects))


def test_diff(projects_path):
    recipe_content = find.recipe_content(
        projects_base=projects_path,
        project='original_chronicle',
        recipe='compute_contracts')

    assert not diff.diff(expected=recipe_content,
                         actual=recipe_content)


def test_diff_nequal():
    simple_content = """first line
second line
third line
"""
    nequal_content = """zero line
first line
SECOND line
third line
"""
    diff_ret = diff.diff(expected=simple_content,
                         actual=nequal_content)
    expected_ret = "--- \n" + "+++ \n" + """@@ -1,3 +1,4 @@
+zero line
 first line
-second line
+SECOND line
 third line
"""
    assert diff_ret == expected_ret
