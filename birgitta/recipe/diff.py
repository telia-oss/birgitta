import difflib

__all__ = ['diff']


def diff(*,
         expected,
         actual):
    """
    Compare two recipe contents.

    Kwargs:
        expected (str): The expected content to compare
        actual (str): The actual content to compare
    Returns:
       None if equal. Otherwise a diff string.
    """

    expected = expected.splitlines(1)
    actual = actual.splitlines(1)

    diff = difflib.unified_diff(expected, actual)
    ret_str = ''.join(diff)
    if ret_str == '':
        return None
    return ret_str
