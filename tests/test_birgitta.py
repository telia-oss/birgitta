"""Tests for `notebookc` package."""
import pytest # noqa 401
from birgitta import birgitta


def test_dummy(capsys):
    """Correct name argument print"""
    birgitta.dummy("Av")
    captured = capsys.readouterr()
    assert f"Printing a dummy line Av" in captured.out
