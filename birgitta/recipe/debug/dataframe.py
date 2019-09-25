"""Debug functions for notebooks.
"""


def count(df, name=None):
    """Print the count of dataframe with title."""
    if name:
        print("Dataset: %s" % (name))
    print("Count: %d" % (df.count()))


def show(df, name=None, num_rows=1):
    """Print title and show a dataframe"""
    if name:
        print("Dataset: %s" % (name))
    df.show(num_rows)


def profile(df, name):
    """Profile a dataframe, initially just count and show."""
    count(df, name)
    show(df, name=name)
