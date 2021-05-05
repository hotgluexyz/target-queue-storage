#!/usr/bin/env python

from setuptools import setup

setup(
    name='target-queue-storage',
    version='1.0.0',
    description='hotglue target for exporting data to Azure Queue Storage',
    author='hotglue',
    url='https://hotglue.xyz',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_blob_storage'],
    install_requires=[
        'azure-storage-queue==12.1.6',
        'argparse==1.4.0'
    ],
    entry_points='''
        [console_scripts]
        target-queue-storage=target_queue_storage:main
    ''',
    packages=['target_queue_storage']
)