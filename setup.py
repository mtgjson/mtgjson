import setuptools

runtime_deps = [
    'aiohttp',
    'bs4',
    'mypy_extensions',
    'requests'
]

dev_deps = [
    'coverage',
    'hypothesis>=3.1.0',
    'mypy',
    'pytest',
    'pytest-asyncio',
    'pytest-cov',
    'tox',
    'vcrpy',
    'yapf'
]


with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='MTGJSON4',
    version=setuptools.depends.get_module_constant('mtgjson4.mtg_globals', '__version__'),
    description='Build JSON files for distribution for Magic: The Gathering',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/mtgjson/mtgjson-python',
    author='Zach Halpern',
    author_email='zahalpern+github@gmail.com',
    license='GPL-3.0',
    classifiers=[
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
    ],
    keywords='Magic: The Gathering, MTG, JSON, Card Games, Collectible, Trading Cards',
    install_requires=runtime_deps,
    extras_require={
        'dev': dev_deps,
    },
)
