#!/usr/bin/env python
import os
from setuptools import find_packages, setup

HERE = os.path.dirname(__file__)
VERSION_FILE = os.path.join(HERE, 'xlsx_provider', 'VERSION')

with open(VERSION_FILE) as f:
    version = f.read().strip()

with open(os.path.join(HERE, "README.md"), "r") as f:
    long_description = f.read()

with open(os.path.join(HERE, "requirements.txt"), "r") as f:
    install_requires = f.read().split("\n")


setup(
    name="airflow-provider-xlsx",
    version=version,
    packages=find_packages(exclude=["scripts", "scripts.*", "tests", "tests.*"]),
    include_package_data=True,
    zip_safe=False,
    url="https://github.com/andreax79/airflow-provider-xlsx",
    author="Andrea Bonomi",
    author_email="andrea.bonomi@gmail.com",
    description="Airflow operators for reading and writing XLSX files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=install_requires,
    license="Apache License, Version 2.0",
    entry_points={
        "apache_airflow_provider": [
            "provider_info=xlsx_provider.__init__:get_provider_info"
        ]
    },
    python_requires=">=3.4",
    classifiers=[
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    test_suite='nose.collector',
    tests_require=['nose'],
)
