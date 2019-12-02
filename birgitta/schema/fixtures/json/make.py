"""Create json version of fixtures.
"""
import importlib
import json
import os
import re
import shutil
from inspect import getmembers, isfunction

from birgitta import spark

__all__ = ['make']


def make(org_mod):
    """Make json fixtures from the fixture definitions
    found in the projects under an organization module.

    Parameters:
    org_mod (module): Full module name of org

    Returns:
    object:Imported python module
"""
    dst_dir = os.path.dirname(org_mod.__file__)
    make_with_dst_dir(org_mod, dst_dir)


def make_with_dst_dir(org_mod, dst_dir):
    """Make json fixtures from the fixture definitions
    found in the projects under an organization module.

    Parameters:
    org_mod (module): Full module name of org

    Returns:
    object:Imported python module
"""
    organization_mod_name = org_mod.__name__
    projects_mod_name = F"{organization_mod_name}.projects"
    projects = mod_from_name(projects_mod_name)
    for proj_mod in submods(projects):
        proj_tests = import_submod(proj_mod, "tests")
        fx_mods = fixture_mods(proj_tests)
        for fx_mod in fx_mods:
            create_json(fx_mod, dst_dir)


def mod_from_name(name):
    """Import module from module name, e.g.
    the newsltd_etl module."""
    return importlib.import_module(name)


def create_json(fx_mod, dst_dir):
    extracts = re.search(
        '.*\\.projects\\.([^.]+)\\.tests\\.fixtures\\.(.*)',
        fx_mod.__name__)
    project = extracts.group(1)
    dataset = extracts.group(2)
    spark_session = spark.local_session()
    fxs = []

    for member in getmembers(fx_mod):
        member_type = member[1]
        member_name = member[0]
        if (isfunction(member_type) and
           member_name.startswith("fx_")):
            fxs.append(member_name)

    for fx in fxs:
        df = getattr(fx_mod, fx)(spark_session)
        json_write(df, project, dataset, fx, dst_dir)


def json_write(df, project, dataset, fx, dst_dir):
    tmp_path = json_tmp_path(project, dataset, fx, dst_dir)
    dst_path = json_dst_path(project, dataset, fx, dst_dir)
    df.coalesce(1).write.mode(
        'append').format("json").save(tmp_path)
    correct_json(tmp_path, dst_path)
    shutil.rmtree(tmp_path)


def correct_json(tmp_path, dst_path):
    records = []
    for file in os.listdir(tmp_path):
        if file.endswith(".json"):
            fpath = "%s/%s" % (tmp_path, file)
            with open(fpath) as fp:
                line = fp.readline().strip()
                while line:
                    records.append(line)
                    line = fp.readline().strip()
    valid_json = "[" + ",".join(records) + "]"
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
    with open(dst_path, "w") as json_file:
        json_file.write(pretty_json(valid_json))


def pretty_json(json_str):
    parsed = json.loads(json_str)
    return json.dumps(parsed, indent=4, sort_keys=True)


def json_tmp_path(project, dataset, fx, dst_dir):
    return json_path(project, dataset, fx, F"{dst_dir}/tmp/")


def json_dst_path(project, dataset, fx, dst_dir):
    return ("%s%s" % (json_path(project, dataset, fx, F"{dst_dir}/"),
                      ".json"))


def json_path(project, dataset, fx, prefix="/"):
    return ("%sprojects/%s/tests/fixtures/generated_json/%s/%s" %
            (prefix, project, dataset, fx))


def fixture_mods(mod):
    mod_dir = list(mod.__path__)[0]
    fx_path = os.path.join(mod_dir, "fixtures")
    if not os.path.isdir(fx_path):
        return []
    else:
        fxs_mod = import_submod(mod, "fixtures")
        return subfiles(fxs_mod)


def import_submod(mod, submod_name):
    sub_mod_address = "%s.%s" % (
        mod.__package__,
        submod_name
    )
    return importlib.import_module(sub_mod_address)


def submods(mod):
    mod_dir = mod.__path__[0]
    submod_names = [mod_name for mod_name in os.listdir(mod_dir)
                    if os.path.isdir(os.path.join(mod_dir, mod_name)) and
                    mod_name != "__pycache__"]
    mods = [import_submod(mod, mod_name)
            for mod_name in submod_names]
    return mods


def subfiles(mod):
    mod_dir = mod.__path__[0]
    submod_names = [mod_name.replace(".py", "")
                    for mod_name in os.listdir(mod_dir)
                    if os.path.isfile(os.path.join(mod_dir, mod_name)) and
                    mod_name not in ["__pycache__", "__init__.py"]]
    mods = [import_submod(mod, mod_name)
            for mod_name in submod_names]
    return mods
