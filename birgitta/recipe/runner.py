# import pkgutil
import inspect
import os
import sys
import traceback

__all__ = ['exec_code', 'run']


def exec_code(code, globals_dict):
    """Execute the recipe code and handle error conditions.

    Args:
        code (str): The code to be executed.
        globals_dict (dict): globals_dict as used by exec().


    Returns:
       Output of python exec() function.
    """
    e = False
    try:
        ret = exec(code, globals_dict)
    except SyntaxError as err:
        error_class = err.__class__.__name__
        # detail = err.args[0]
        lineno = err.lineno
        e = err
    except Exception as err:
        error_class = err.__class__.__name__
        # detail = err.args[0]
        cl, exc, tb = sys.exc_info()
        lineno = traceback.extract_tb(tb)[-1][1]
        e = err
    if e:
        lines = code.splitlines()
        lenlines = len(lines)
        if lineno < lenlines:
            print("Recipe execution",
                  error_class,
                  "at recipe line:\n",
                  lines[lineno-1])
        else:
            print("Recipe execution",
                  error_class)
        raise e
    return ret


def run(root_mod, recipe, storage, replacements=[]):
    """Obtain a dataframe. It will adjust to whatever
    storage the environment has set. Currently storage is supported in
    file, memory or dataiku (HDFS).

    Args:
        root_mod (module): The root module on which to base the path.
        recipe (str): Relative path to the recipe file from the module dir.
        storage (str): Storage type e.g. DATAIKU, FILE
        replacements (list): List of text replacements to enable recipe
        debugging. Example on how to limit data amount:

        [
            {
                "old": "dataframe.get(sql_context, ds_foo.name)",
                "new": "dataframe.get(sql_context, ds_foo.name).limit(10)"
            }
        ]

    Returns:
       Output of python exec() function.
    """
    if storage not in ["DATAIKU", "FILE"]:
        raise ValueError(f"Storage unknown {storage}")
    mod_path = os.path.dirname(inspect.getfile(root_mod))
    recipe_path = f"{mod_path}/{recipe}"
    with open(recipe_path) as f:
        code = prepare_code(f.read(), recipe, storage, replacements)
    globals_dict = {
        'BIRGITTA_DATASET_STORAGE': storage
    }
    return exec_code(code, globals_dict)


def prepare_code(code, recipe, storage, replacements):
    for replacement in replacements:
        code = code.replace(replacement["old"], replacement["new"])
    glob_stmts = f"""import glob
from birgitta import glob
glob.set("BIRGITTA_DATASET_STORAGE", "{storage}")"""
    completed = f"""
print("=== Recipe {recipe} complete ===")"""
    return glob_stmts + code + completed
