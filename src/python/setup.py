#!/usr/bin/env python3

from setuptools import setup, find_packages

setup(
    name='dataAlgorithms-hadoop-streaming',
    version='0.0.1',
    description='python package for hadoop streaming API',
    packages=find_packages(),
    install_requires=[
        'pyspark=2.4.5',
    ],
    python_requires='>=3.8.0'
)
