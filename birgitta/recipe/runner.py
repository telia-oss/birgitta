# import pkgutil
import inspect
import os
import sys
import traceback

from birgitta.dataframesource import contextsource
from pyspark.sql.utils import AnalysisException

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
        err_desc = err.__class__.__name__
        # detail = err.args[0]
        lineno = err.lineno
        e = err
    except AnalysisException as err:
        err_desc = err.__class__.__name__
        lineno = get_lineno(err)
        e = err
    except NameError as err:
        err_desc = f'{err.__class__.__name__}: {err}'
        lineno = get_lineno(err) - 1
        if lineno < 0:
            lineno = 0
        e = err
    except Exception as err:
        err_desc = err.__class__.__name__
        lineno = get_lineno(err)
        e = err
    if e:
        lines = code.splitlines()
        lenlines = len(lines)
        if lineno < lenlines:
            print("\nRecipe execution",
                  err_desc,
                  "\n",
                  guilty_lines(lines, lineno))
        else:
            print("Recipe execution",
                  err_desc)
        raise e
    return ret


def get_lineno(err):
    # detail = err.args[0]
    cl, exc, tb = sys.exc_info()
    tracebacks = traceback.extract_tb(tb)
    filename = None
    for tback in tracebacks:
        filename = tback[0]
        if filename == '<string>':  # Found exec()'ed string entry
            lineno = tback[1]
            break
    if not filename:
        # Fall back to last line, if filename == '<string>' not found
        lineno = tracebacks[-1][1]
    return lineno


def guilty_lines(lines, lineno):
    """Print the error line, including previous and next lines"""
    # Previous lines
    ret_lines = [""]  # Add spacing
    non_prefix = "         "
    err_prefix = "ERROR -> "
    for i in range(-6, 0):
        prev_lineno = lineno + i
        if prev_lineno >= 0:
            line = lines[prev_lineno]
            if line.startswith('log_transform('):
                continue  # Skip injected log lines
            ret_lines.append(printable_line(non_prefix,
                                            prev_lineno,
                                            line))
    # Error line
    ret_lines.append(printable_line(err_prefix, lineno, lines[lineno]))
    # Next lines
    lenlines = len(lines)
    for i in range(1, 7):
        next_lineno = lineno + i
        if next_lineno < lenlines:
            line = lines[next_lineno]
            if line.startswith('log_transform('):
                continue  # Skip injected log lines
            ret_lines.append(
                printable_line(non_prefix, next_lineno, line))
    ret_lines.append("")  # Add spacing
    return "\n".join(ret_lines)


def printable_line(prefix, lineno, line):
    # FUTURE: Detract script prepend size from lineno, so lineno corresponds
    # to recipe code line no
    # return f"{non_prefix} {prev_lineno}: {lines[prev_lineno]}"
    return f"{prefix}: {line}"


def run(root_mod, recipe, *, dataframe_source=None, replacements=[]):
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
    if not dataframe_source:
        dataframe_source = contextsource.get()
    globals_dict = {
        'BIRGITTA_DATAFRAMESOURCE': dataframe_source
    }
    return exec_code(code, globals_dict)


def run_and_exit(root_mod, recipe, *, dataframe_source=None, replacements=[]):
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
    ret = run(root_mod,
              recipe,
              dataframe_source=dataframe_source,
              replacements=replacements)
    print(ret)
    rpath = recipe_path(root_mod, recipe)
    print(f"Exit after successfully running recipe: {rpath}")
    sys.exit(0)


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
