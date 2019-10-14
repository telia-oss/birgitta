import pytest
# from birgitta import spark
from birgitta import glob
from birgitta.recipe import runner
from examples.organizations.newsltd.projects import tribune


def test_run():
    glob.reset()
    with pytest.raises(TypeError):
        runner.run(tribune, "recipes/compute_filtered_contracts.py", "FILE")


def test_syntax_error():
    glob.reset()
    replacements = [
        {
            "old": "sql_context = ",
            "new": "sql_contesdaf@_0=~> = "
        }
    ]
    with pytest.raises(SyntaxError):
        runner.run(tribune,
                   "recipes/compute_filtered_contracts.py",
                   "FILE",
                   replacements)
