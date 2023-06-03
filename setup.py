from setuptools import setup, find_packages

setup(
    name='mq-raft-demo',
    version='1.0.0',
    packages=find_packages(),
    install_requires=[
        'iniconfig==2.0.0',
        'packaging==23.1',
        'pluggy==1.0.0',
        'pytest==7.3.1',
    ],
)
