from distutils.core import setup
from setuptools import find_packages
from itab import __version__, __author__, __author_email__

setup(
    name="itab",
    version=__version__,
    packages=find_packages(),
    author=__author__,
    author_email=__author_email__,
    description="Python tab files parsing and validating schema tools.",
    license="Apache License 2",
    keywords="",
    url="https://github.com/bbglab/itab",
    long_description=__doc__,
    install_requires=['six',],
    entry_points={
        'console_scripts': [
            'tsvcheck = itab.utils.tsvcheck:cmdline'
        ]
    }
)
