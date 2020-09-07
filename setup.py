# -*- coding: utf-8 -*-
from setuptools import find_packages
from setuptools import setup
import os

version = '0.8.0'
shortdesc = "DB sync"


setup(
    name='bddbsync',
    version=version,
    description=shortdesc,
    # long_description=longdesc,
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Topic :: Software Development',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords='database sql sync',
    author='BlueDynamics Alliance',
    author_email='phil@bluedynamics.com',
    url='http://github.com/zworkb/python-sync-db',
    license='MIT',
    packages=find_packages(),
    # package_dir={'': 'src'},
    include_package_data=True,
    zip_safe=True,
    install_requires=[
        'sqlalchemy',
        'websockets',
        'requests',
    ],
)
