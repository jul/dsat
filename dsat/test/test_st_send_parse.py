#!/usr/bin/env python
# -*- codong: utf-8 -*-

import unittest
from unittest import TestCase
import zmq
from zmq.tests import BaseZMQTestCase, have_gevent, GreenTest
from time import sleep
from dsat.message import send_vector, parse_event

class TestSendParse(BaseZMQTestCase):

    def test_consistency(self):
        s1, s2 = self.create_bound_pair(zmq.PUSH,zmq.PULL)
        sleep(.1)
        msg = dict( 
                job_id=1,
                task_id=1,
                seq = 1,
                where = "here",
                next = "generation",
                wid = 2,
                event = "whatever",
                state = "nawak",
                step = "this",
                pid = 123,
                arg = dict( a = 1, b = 2),
                type = "test",
        )
        send_vector(s1, msg)
        v = parse_event(s2)
        self.assertEqual(v["seq"], "2")
        for k,v in msg.items():
            if "seq" != k:
                self.assertEqual(v, msg[k])


if __name__ == '__main__':
    unittest.main(verbosity=4)


