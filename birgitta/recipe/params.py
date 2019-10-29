from birgitta import context


def today():
    """Returns fixed value for today to enable consistent tests.
    """
    return context.get("TODAY")
