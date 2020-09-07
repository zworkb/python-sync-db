# -*- coding: utf-8 -*-
from setuptools import find_packages
from setuptools import setup
import os

version = '0.8.0'
shortdesc = "DB sync"

breakpoint()

setup(
    name='bddbsync',
    version=version,
    description=shortdesc,
    keywords='database sql sync',
    author='BlueDynamics Alliance',
    author_email='phil@bluedynamics.com',
    url='http://github.com/zworkb/python-sync-db',
    license='MIT',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=True,
    install_requires=[
        'sqlalchemy',
        'websockets',
        'requests',
        'rfc3339',
    ],
)
