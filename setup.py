#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open("README.md") as readme_file:
    readme = readme_file.read()

# read version.py
import sys, re

try:
    filepath = "qsec/version.py"
    version_file = open(filepath)
    (__version__,) = re.findall('__version__ = "(.*)"', version_file.read())

except Exception as error:
    __version__ = "0.0.1"
    sys.stderr.write("Warning: Could not open '%s' due %s\n" % (filepath, error))

requirements = ["pyarrow"]

setup(
    name="qsec",
    version=__version__,
    python_requires=">=3.6",
    packages=find_packages(),
    scripts=[],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Scientific/Engineering :: Medical Science Apps",
    ],
    description=("Quant & Systematic Crypto Research Tools"),
    install_requires=requirements,
    license="BSD-3-Clause License",
    long_description=readme,
    long_description_content_type="text/markdown",
    include_package_data=True,
    keywords="cryptocurrency, binance",
    zip_safe=False,
)
