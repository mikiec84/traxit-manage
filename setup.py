#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import find_packages


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


with open('README.rst') as readme_file:
    readme = readme_file.read()

from pip.req import parse_requirements
requirements = [str(i.req) for i in parse_requirements("requirements.txt", session=False)]
test_requirements = [str(i.req) for i in parse_requirements("test_requirements.txt", session=False)]

VERSION = '0.10'

setup(
    name='traxit_manage',
    version=str(VERSION),
    description="Organize your audio files in a specific structure. Use scripts or a CLI to run tests.",
    long_description=readme,
    author="Flavian Hautbois",
    author_email='flavian@trax-air.com',
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    include_package_data=True,
    install_requires=requirements,
    zip_safe=False,
    keywords='traxair',
    entry_points={
            "console_scripts": [
                "traxit=traxit_manage.bin:main",
            ],
        },
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
    ],
    extra_requires = {
                      'celery': ['traxit_celery>=1.0.0,<1.1.0'],
                      'algorithm': ['traxit_algorithm>=1.0.0,<1.1.0'],
                      'all': ['traxit_celery>=1.0.0,<1.1.0',
                               'traxit_algorithm>=1.0.0,<1.1.0'],
                      },
    test_suite='tests',
    tests_require=test_requirements
)
