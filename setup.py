import setuptools

setuptools.setup(
    name='MTGJSON4',
    version=setuptools.depends.get_module_constant('mtgjson4', '__VERSION__'),
    author=setuptools.depends.get_module_constant('mtgjson4', '__MAINTAINER__'),
    author_email=setuptools.depends.get_module_constant('mtgjson4', '__MAINTAINER_EMAIL__'),
    url=setuptools.depends.get_module_constant('mtgjson4', '__REPO_URL__'),
    description='Build JSON files for distribution for Magic: The Gathering',
    long_description=open('README.md', 'r').read(),
    long_description_content_type='text/markdown',
    license='GPL-3.0',
    classifiers=[
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
    ],
    keywords='Magic: The Gathering, MTG, JSON, Card Games, Collectible, Trading Cards',
    packages=setuptools.find_packages(),
    install_requires=[
        'bs4',
        'requests',
    ],
)
