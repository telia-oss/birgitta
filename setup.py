from codecs import open
from os import getenv
from os import path

from setuptools import find_packages
from setuptools import setup


version = getenv('APP_VERSION')
here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

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
        'Topic :: Machine Learning/Data Engineering',
    ],

    keywords='',

    packages=find_packages(exclude=[
        'contrib',
        'docs',
        '*.tests',
        '*.tests.*',
        'tests.*',
        'tests',
        'birgitta/tests.*',
        'birgitta/tests'
        ]),

    # List of dependencies, with exact versions
    install_requires=[
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
)
