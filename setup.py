from setuptools import find_packages
from setuptools import setup

setup(name='bddbsync',
      version="0.8.0",
      url='https://github.com/bintlabs/python-sync-db',
      author='Bint',
      packages=['dbsync', 'dbsync.client', 'dbsync.server', 'dbsync.messages'],
      # packages=find_packages('dbsync'),
      # package_dir={'': 'dbsync'},
      # include_package_data=True,
      description='Centralized database synchronization for SQLAlchemy',
      install_requires=[
            'sqlalchemy',
            'requests',
            'nose'
            ],
      license='MIT',)
