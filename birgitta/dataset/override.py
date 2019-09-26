"""Functions for enabling name overrides.

Used to enable splitting dev, test and prod data sets.
Useful for running recipe tests in dataiku with test
data sets instead of regular data sets."""
from birgitta import glob


def set_override(orig_name, override):
    if not glob.get("BIRGITTA_DATASET_OVERRIDES"):
        glob.set("BIRGITTA_DATASET_OVERRIDES", {})
    glob.Glob.BIRGITTA_DATASET_OVERRIDES[orig_name] = override


def override_if_set(orig_name):
    overrides = glob.get("BIRGITTA_DATASET_OVERRIDES")
    if (overrides and orig_name in overrides):
        return overrides[orig_name]
    else:
        return orig_name
