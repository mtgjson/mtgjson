import os

from setuptools import setup, find_packages

from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Direct dependencies; minimal set that this should be compatible with.
# Pinned dependencies that we've tested with are present in requirements.txt
runtime_deps = [
    'aiohttp',
    'bs4'
]

dev_deps = [
    'coverage',
    'hypothesis==3.1.0',
    'mypy',
    'pytest',
    'pytest-asyncio',
    'pytest-cov',
    'tox',
    'vcrpy',
    'yapf'
]

setup(
    name='MTGJSON4',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version='4.0.0',

    description='Build JSON files for distribution for Magic: The Gathering',
    long_description='Create JSON files of Magic: The Gathering cards for distribution from sources such as Gatherer',

    # The project's main homepage.
    url='https://github.com/mtgjson/mtgjson-python',

    # Author details
    author='Zach Halpern',
    author_email='zahalpern+github@gmail.com',

    # Choose your license
    license='GPL-3.0',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)'
    ],

    # What does your project relate to?
    keywords='Magic: The Gathering, MTG, JSON, Card Games, Collectible, Trading Cards',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=[find_packages()],

    # Alternatively, if you want to distribute just a my_module.py, uncomment
    # this:
    #   py_modules=["my_module"],

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=runtime_deps,

    # List additional groups of dependencies here (e.g. development
    # dependencies). You can install these using the following syntax,
    # for example:
    # $ pip install -e .[dev,test]
    extras_require={
        'dev': dev_deps
    },

    # If there are data files included in your packages that need to be
    # installed, specify them here.  If using Python 2.6 or less, then these
    # have to be included in MANIFEST.in as well.
    package_data={ },

    # Although 'package_data' is the preferred approach, in some case you may
    # need to place data files outside of your packages. See:
    # http://docs.python.org/3.4/distutils/setupscript.html#installing-additional-files # noqa
    # In this case, 'data_file' will be installed into '<sys.prefix>/my_data'
    data_files=[],

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points={},
)
