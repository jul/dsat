#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
import unittest
import sys
import os
sys.path += [ "./dsat" ]

import dsat
#TODO call make text with sphinx (please don't use system) before
#building/install

fcontent = lambda f: open(f).read()

def test():
    loader= unittest.TestLoader()
    suite=loader.discover("")
    runner=unittest.TextTestRunner()
    result=runner.run(suite)
    if not result.wasSuccessful():
        raise Exception( "Test Failed: Aborting install")
long_desc = fcontent('README.rst')
setup(
        name = "dsat", 
        version = dsat.__version__,
        description = "distributed system for adaptative worker/process",
        packages = find_packages(),
        package_dir = dict( dsat = "dsat",),
        install_requires = map(
            str.strip,
            open("requirements.txt").readlines()
        ),
        ### doc writer don't be stupid
        ### if there is an order for the file then use alphabetical
        license = fcontent('LICENSE.txt'),
        long_description = fcontent('README.rst'),
        classifiers = [
            'Development Status :: 3 - Alpha',
            'Programming Language :: Python :: 2.7',
            'License :: OSI Approved :: BSD License',
            'Environment :: Console',
            'Operating System :: POSIX :: Linux',
            'Topic :: System :: Distributed Computing'
        ],
        test_suite="tests",
)

if 'install' in sys.argv:
    test()
    print long_desc

