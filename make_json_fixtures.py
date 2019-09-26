import re

from birgitta.schema.fixtures import json as fx_json


def repo_root(file_path):
    return re.search('(^.*/birgitta)/', file_path)[1]


def org_path():
    repo_dir = repo_root(__file__)
    return F"{repo_dir}/examples/organizations/newsltd"


fx_json.make('examples.organizations.newsltd', org_path())
