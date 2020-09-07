from setuptools import setup, find_packages

setup(
    name='bddbsync',
    version='0.8.0',
    url='https://github.com/zworkb/python-sync-db',
    author='Author Name',
    author_email='phil@bluedynamics.com',
    description='Database synchronisation',
    packages=find_packages(),
    install_requires=['sqlalchemy', 'websockets', 'requests'],
)