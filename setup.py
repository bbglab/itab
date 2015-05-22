from distutils.core import setup
from setuptools import find_packages
from itab import __version__, __author__, __author_email__

setup(
    name="itab",
    version=__version__,
    packages=find_packages(),  # metadata
    author=__author__,
    author_email=__author_email__,
    description="Python tab separated values files toolkit",
    license="Apache License 2",
    keywords="",
    url="https://github.com/jordeu/itab",
    long_description=__doc__,
    install_requires=[],
    entry_points={
        'console_scripts': [
            'itab = itab.main:cmdline'
        ]
    }
)
