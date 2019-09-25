from birgitta import glob


def today():
    """Returns fixed value for today to enable consistent tests.
    """
    return glob.get("BIRGITTA_DATASET_OVERRIDES")
