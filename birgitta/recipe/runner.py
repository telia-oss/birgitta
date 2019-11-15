# import pkgutil
import inspect
import os
import sys
import traceback

__all__ = ['exec_code', 'run', 'run_and_exit']


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


def run(root_mod, recipe, dataframe_source, replacements=[]):
    """Run a recipe stored in .py file with exec(). The path is relative
    to the path of the mod.

    Args:
        root_mod (module): The root module on which to base the path.
        recipe (str): Relative path to the recipe file from the module dir.
        dataframe_source (DataframeSourceBase subclass): dataframe source
            E.g. LocalSource, Dataiku
        replacements (list): List of text replacements to enable recipe
        debugging. Example on how to limit data amount:

        [
            {
                "old": "dataframe.get(spark_session, ds_foo.name)",
                "new": "dataframe.get(spark_session, ds_foo.name).limit(10)"
            }
        ]

    Returns:
       Output of python exec() function.
    """
    rpath = recipe_path(root_mod, recipe)
    with open(rpath) as f:
        code = prepare_code(f.read(), recipe, replacements)
    globals_dict = {
        'BIRGITTA_DATAFRAMESOURCE': dataframe_source
    }
    return exec_code(code, globals_dict)


def run_and_exit(root_mod, recipe, dataframe_source, replacements=[]):
    """Run a recipe stored in .py file with exec(). The path is relative
    to the path of the mod. When finished exit(). This is a utility function
    to shortcut a recipe, and leave the rest of the recipe unexecuted.
    This way the recipe can easily be reenabled if further hacking is needed.

    Args:
        root_mod (module): The root module on which to base the path.
        recipe (str): Relative path to the recipe file from the module dir.
        dataframe_source (DataframeSourceBase subclass): dataframe source
            E.g. LocalSource, Dataiku
        replacements (list): List of text replacements to enable recipe
        debugging. Example on how to limit data amount:

        [
            {
                "old": "dataframe.get(spark_session, ds_foo.name)",
                "new": "dataframe.get(spark_session, ds_foo.name).limit(10)"
            }
        ]

    Returns:
       None. Prints output and calls sys.exit().
    """
    ret = run(root_mod, recipe, dataframe_source, replacements)
    print(ret)
    rpath = recipe_path(root_mod, recipe)
    sys.exit(f"Exit after running recipe: {rpath}")


def prepare_code(code, recipe, replacements):
    for replacement in replacements:
        code = code.replace(replacement["old"], replacement["new"])
    context_stmts = f"""from birgitta.dataframesource import contextsource
contextsource.set(globals()['BIRGITTA_DATAFRAMESOURCE'])
"""
    completed = f"""
print("=== Recipe {recipe} complete ===")
"""
    return context_stmts + code + completed


def recipe_path(root_mod, recipe):
    mod_path = os.path.dirname(inspect.getfile(root_mod))
    return f"{mod_path}/{recipe}"
