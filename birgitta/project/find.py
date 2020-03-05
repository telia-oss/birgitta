"""find() finds the path of a project within projects path.
"""
import glob
import re

__all__ = ['find']


def find(*, path, name):
    """Look for Project("proj_foo") in __init__.py files
    under path.

    Since this function should be called to compare code
    in one git repo with another repo, we do the search in text,
    not by loading the project modules as python modules.
    The dependencies for the project modules might not be
    present. In any case, we are only interested in checking
    if the content is equal.

    Kwargs:
        path (str): base path for alll projects e.g.
        '/path/to/newsltd_etl/projects'.
        name (str): Project name, as defined in Project('projfoo_name')
    Returns:
       Path of project directory.
    """
    # Looks through __init__.py files for project constructors.
    i_suffix = '/__init__.py'
    inits = glob.glob(f"{path}/*{i_suffix}")
    for i in inits:
        name_in_init = proj_name_from_init(i)
        if name == name_in_init:
            suf_len = len(i_suffix)
            proj_path = i[0:-suf_len]
            return proj_path
    raise ValueError(f"Project {name} not found in {path}")


def proj_name_from_init(i):
    """Get the project name from __init__.py path i

    Args:
        i (str): path of a __init__.py file
    Returns:
        name from Project('projfoo_name') statement or None.
    """
    with open(i) as i_search:
        for line in i_search:
            matches = re.match("Project\('([^']+)'\)", line)  # noqa W605
            if matches:
                return matches[1]
    return None
