#!/usr/bin/env python

from sys import stdout
from time import sleep

import unittest
from dsat.message import _filter_dict
from picomongo import ConnectionManager

class TestFilterDict(unittest.TestCase):
    def setUp(self):
        self.a_dict = dict(
                a_key=1,
                _hidden = 2,
        )

    def test_filter(self):
        self.assertEqual(
                _filter_dict(self.a_dict),
                dict(a_key = 1)
        )

try:
    ConnectionManager.configure(
        {
            '_default_' : {
                'uri' : 'mongodb://localhost/',
                'db' : 'state',
            },
            'state': {
### http://docs.mongodb.org/manual/reference/connection-string/
                'col' : 'state',
            }
        }
    )

except Exception as e:
    import sys
    sys.stderr.write("*" * 70 +
            "\nTestState DISABLED no local instance available\n" +
            "*" * 70 + "\n" * 2)
    del( TestState )

if __name__ == '__main__':

    unittest.main(verbosity=4)

