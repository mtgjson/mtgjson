"""
Installation setup for mtgjson5
"""
import configparser
import pathlib

import setuptools

config = configparser.RawConfigParser()
config.read(str(pathlib.Path(__file__).resolve().parent.joinpath("mtgjson.properties")))

setuptools.setup(
    name="mtgjson5",
    version=config.get("MTGJSON", "version"),
    author="Zach Halpern",
    author_email="zach@mtgjson.com",
    url="https://mtgjson.com/",
    description="Magic: the Gathering compiled database generator",
    long_description=pathlib.Path("README.md").open().read(),
    long_description_content_type="text/markdown",
    license="MIT",
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows :: Windows 10",
        "Operating System :: Unix",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Software Development :: Version Control :: Git",
    ],
    keywords=[
        "Big Data",
        "Card Games",
        "Collectible",
        "Database",
        "JSON",
        "MTG",
        "MTGJSON",
        "Trading Cards",
        "Magic: The Gathering",
    ],
    include_package_data=True,
    packages=setuptools.find_packages(),
    install_requires=pathlib.Path("requirements.txt").open().readlines(),
)
