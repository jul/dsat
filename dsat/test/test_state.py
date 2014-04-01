#!/usr/bin/env python

from sys import stdout
from time import sleep

import unittest
from dsat.message import State, _filter_dict
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

class TestState(unittest.TestCase):
    def setUp(self):
        self.base_config = {
            "backend": {
                "_default_" : {
                    "uri" : "mongodb://localhost/", "db" : "test_state" },
                "state": { "col" : "test_state" }
            }
        }
        if not hasattr(self, "write_state"):
            self.write_state = State(self.base_config,
                [
                    dict(
                        job_id = "no",
                        arg =  {},
                        task_id = "where"
                    ),
                ])
        else:
            self.write_state.add( 
                    dict(
                        job_id = "no",
                        arg =  {},
                        task_id = "where"
                    ),
            )

        self.assertEqual(1,
                len([x for x in self.write_state.Backend.col.find()]))
        self.write_state.delete("no","where") 
        self.assertEqual(0,
                len([x for x in self.write_state.Backend.col.find()]))
        self.write_state.Backend.col.drop()

    def test_singleton_logic(self):
        """test you cannot instanciate two writers (integrity)"""
        with self.assertRaisesRegexp(ValueError,
        "this class should not be instanciated twice"):
            s = State(self.base_config)
        # but you have the right to instanciate readonly instances
        cfg = self.base_config
        cfg["readonly"] = True
        read_state1 = State(cfg)
        self.assertTrue(True)
        read_state2 = State(cfg)
        self.assertTrue(True)

    def test_readonly_disabled_method(self):
        """some methods are forbidden in readonly"""
        cfg = self.base_config
        cfg["readonly"] = True
        read_state = State(cfg)
        for method in {# "pending_task", "is_active_job", NOT USED 
                "save", "update",
                "add" }:
            with self.assertRaisesRegexp(Exception,"Not implemented"):
                getattr(read_state, method)()

    def test_get_consistency(self):
        """get should work with same API for readonly/write State"""
        cfg = self.base_config
        cfg["readonly"] = True
        read_state2 = State(cfg)


        with self.assertRaises(AssertionError):
            self.write_state.add( dict( 
                job_id=1 , task_id=2, arg={}
            ))
        
        s = self.write_state.add(dict( 
            job_id="1", task_id="2", arg={}
        ))
        with self.assertRaisesRegexp(Exception,"Integrity error"):
            self.write_state.add(dict( 
                job_id="1", task_id="2", arg={}
            ))
        print( "sleeping to avoid race condition")
        sleep(2)
        for  label,state in [ ("RW",self.write_state),("R", read_state2) ]:
            print( label )
            ## the simple API
            self.assertTrue(state.get("1","2"))
            ## the dict API
            self.assertTrue(state.get({"job_id" : "1", "task_id" : "2"}))
            self.assertTrue(state.get(
                    {"job_id" : "1", "arg" : 2, "task_id" : "2"}))
            ## Errors
            with self.assertRaises(KeyError):
                self.assertFalse(state.get("not","there"))
            with self.assertRaises(ValueError):
                state.get(1)

        self.assertTrue("what" in  self.write_state.update(
                dict(
                    job_id="1" , task_id="2", arg={}, what = "wut",
                )
            )
        )
        record = self.write_state.update(
                dict(
                    job_id="1" , task_id="2", arg={}, what = "wut",
                )
            )
       
        sleep(.1)
        self.assertEqual(
                "wut", 
                read_state2.get({"job_id" : "1", "task_id" : "2"})["what"]
        )

        self.write_state.save()


    def test_read_write_consitency(self):
        """scenario of set/get ..."""
        self.write_state.add(dict( 
            job_id="1" , task_id="2", arg={}
        ))
        #self.assertTrue( "2" in self.write_state.pending_task())
        #self.assertTrue( self.write_state.is_active_job("1") )
        self.assertTrue( self.write_state.get("1","2"))



    def tearDown(self):
        self.write_state.Backend.col.drop()
        #### BUG PUT IN DESTRCUTOR (__del__)
        State.AlreadySet = False
        del(self.write_state)


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

