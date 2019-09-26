"""Timing functions for recipetest.
"""
from datetime import datetime

__all__ = ['time', 'print_time', 'print_results']

first = datetime.now()
last = first
results = []


def time(description=""):
    """Add timing entry with description

    Args:
        description (str): Description for this entry.
    """
    global last, first, results
    entry = {
        "str": str,
        "last": last,
        "first": first,
        "now": datetime.now()
    }
    results.append(entry)
    last = entry["now"]


def print_time(entry):
    """Print the time for a single entry"""
    print("spent:",
          diffstr(entry["last"], entry["now"]),
          "total:",
          diffstr(entry["first"], entry["now"]),
          "now: ",
          entry["now"].strftime("%H:%M:%S.%f"),
          entry["str"])


def print_results(case=""):
    """print all the results for case"""
    global results
    print("=============== Time profile: %s ================\n" % (case))
    for entry in results:
        print_time(entry)


def diffstr(a, b):
    diff = b - a
    return str(diff)
