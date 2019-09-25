"""
Usage:
  birgitta -h | --help
  birgitta json-fixtures <organization-module> <dst-dir>

birgitta json-fixtures Creates json fixtures from the fixture
definitions in all datasets in all projects of the organization
module. The fixtures are writte to <dst-dir>

Arguments:
  <organization-module> The organization module, e.g.
  'examples.organizations.newsltd'. It should have a sub module
  called 'projects', which in turn holds:

    * shared schemas and field catalogs
    * project modules containing recipes, data set schemas and fixture definitions

  See example organization here:
  https://github.com/telia-oss/birgitta/tree/master/examples/organizations/newsltd/

  <dst-dir> Directory path where json fixtures should be stored. By convention, this
  should be the path to your organization, so that fixtures end up under their respective
  project test catalogs, e.g.
  examples/organizations/newsltd/projects/tribune/tests/fixtures/generated_json/filtered_contracts/fx_default.json

Options:
  -h, --help             Display this message
"""  # noqa E501

from birgitta.schema.fixtures import json as fx_json
from docopt import docopt

__all__ = ['main']


def main():
    args = docopt(__doc__)
    if args['json-fixtures']:
        org_module_name = args['<organization-module>']
        dst_dir = args['<dst-dir>']
        fx_json.make(org_module_name, dst_dir)


if __name__ == "__main__":
    main()
