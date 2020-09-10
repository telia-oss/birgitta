from birgitta.project import find


__all__ = ['recipe_content', 'recipe_path']


def recipe_content(*,
                   projects_base,
                   project,
                   recipe):
    """
    Returns the content of a recipe in a project, based on the
    projects base path, and the project and recipe name.

    Kwargs:
        projects_base (str): base path for all projects, e.g.
        '/path/to/newsltd_etl/projects'.
        project (str): Project name, as defined in Project('projfoo_name')
        recipe (str): Recipe name e.g. "compute_contracts",
        as part of recipe file name "compute_contracts.py"
    Returns:
       True or False
    """
    rpath = recipe_path(projects_base=projects_base,
                        project=project,
                        recipe=recipe)
    return open(rpath, "r").read()


def recipe_path(*, projects_base, project, recipe):
    """
    Returns the path of a recipe in a project, based on the
    projects base path, and the project and recipe name.
    Kwargs:
        projects_base (str): base path for all projects, e.g.
        '/path/to/newsltd_etl/projects'.
        project (str): Project name, as defined in Project('projfoo_name')
        recipe (str): Recipe name e.g. "compute_contracts",
        as part of recipe file name "compute_contracts.py"
    Returns:
       True or False
    """
    project_path = find(
        path=projects_base,
        name=project)
    recipe_path = f"{project_path}/recipes/{recipe}.py"
    return recipe_path
