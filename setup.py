from os import path

from setuptools import find_packages
from setuptools import setup


version = '0.1.32'
here = path.abspath(path.dirname(__file__))

long_description = """Birgitta is a Python ETL test and schema framework,
providing automated tests for pyspark notebooks/recipes.

Birgitta allows doing solid ETL and ML, while still liberally
allowing imperfect notebook code, enabling a
`DataOps <https://www.dataopsmanifesto.org>` way of
working which is both solid and agile, not killing
Data Scientist flexibility by excessive coding standards in notebooks.

In addition to running recipetests on your local dev machine or on
a CI/CD server, there is support for running recipetests
as [Dataiku](https://www.dataiku.com] DSS Scenarios.
"""

setup(
    name='birgitta',

    version=version,

    description='Pyspark and notebook unit testing, especially focused on Dataiku.', # noqa 501
    long_description=long_description,

    url='https://github.com/telia-oss/birgitta',

    author='Telia Norge',

    license='MIT',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],

    keywords='',

    packages=find_packages(exclude=[
        'contrib',
        'docs',
        '*.tests',
        '*.tests.*',
        'tests.*',
        'tests'
        ]),

    # List of dependencies, with exact versions
    install_requires=[
        'docopt',
        'numpy',
        'pyspark',
        'mock',
        'pandas',
        'pyarrow',
        'pytest',
        'pytest-cov',
        'pytest_mock',
        'holidays'
    ],

    include_package_data=True,
    package_data={},
    entry_points={
        'console_scripts': [
        ]
    }
)
