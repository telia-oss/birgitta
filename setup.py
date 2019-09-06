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
            'numpy==1.15.2',
            'pyspark==2.3.1',
            'mock==2.0.0',
            'pandas==0.24.1',
            'pyarrow==0.13.0',
            'pytest==3.8.0',
            'pytest-cov==2.5.1',
            'pytest_mock==1.10.0',
            'holidays==0.9.10'
    ],

    # List additional groups of dependencies here (e.g. development
    # dependencies). You can install these using the following syntax,
    # for example:
    # $ pip install -e .[dev,test]
    extras_require={
        'dev': [
            'flake8==3.7.7',
            'flake8-import-order==0.18',
            'jupyter==1.0.0',
            'pip==19.0.3',
            'wheel==0.33.1'
        ]
    },

    include_package_data=True,
    package_data={},
)
