"""
Installation setup for mtgjson5
"""
import setuptools


setuptools.setup(
    name="mtgjson5",
    version=setuptools.depends.get_module_constant(
        "mtgjson5.consts", "MTGJSON_VERSION"
    ),
    author="Zach Halpern",
    author_email="zach@mtgjson.com",
    url="https://mtgjson.com/",
    description="Magic: the Gathering compiled database generator",
    long_description=open("README.md").read(),
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
    install_requires=open("requirements.txt").read().splitlines(),
)
