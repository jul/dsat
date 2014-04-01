#!/usr/bin/env python
# -*- coding: utf-8 -*-
from functools import wraps
from calendar import timegm
from time import gmtime, sleep
from os import path, makedirs
import shutil
import logging
from collections import defaultdict, MutableMapping
#from json import loads, dumps, dump
from types import StringType
from zmq.utils.jsonapi import dumps
from json import loads
from picomongo import ConnectionManager, Document
from pymongo.errors import DuplicateKeyError
from pymongo import ASCENDING, DESCENDING
from time import sleep, time
from exceptions import KeyError

"""Function and class related to process control and messaging"""


__all__ = [ "incr_task_id", "_filter_dict", "parse_event", "send_vector", "send_event", "State",  "incr_seq" ]


_SENTINEL = object()


def _filter_dict(a_dict):
    return { str(k):w for k,w in a_dict.items() if not k.startswith("_") }

WHERE_FORMAT =  "{where}:{step}:{wid}:{pid}:{next}"
WHAT_FORMAT = "{type}:{job_id}:{task_id}:{event}:{seq}"

def incr_task_id(vector):
    vector["task_id"]= str(( int(vector["task_id"])+1 ))
    
def decr_seq(vector):
    vector["seq"]= str(( int(vector["seq"])-1 ))

def incr_seq(vector):
    vector["seq"]= str(( int(vector["seq"])+1 ))


def parse_event(zmq_socket):
    """Takes a well configure socket and returns the state.
    
    The returned vector contains:
        * job_id = the name of the measurement campaign
        * task_id = the special unique_id of this measure
        * where = on which server
        * step = the worker's circus name that is actually suppose to consume
        * next the next step to send (info)
        * wid: circus worker ID
        * seq: message sequence
        * arg: a dict with the arguments for the stage
        * pid
        * event the event that is sent

    """
    null_joined_string = zmq_socket.recv()
    where, step, when, arg = null_joined_string.split("\x00")
    ### I like to play with stuff
    _type, job_id, task_id, event, seq = step.split(":")
    where, step, wid, pid, _next = where.split(":")
    return dict(
            job_id = job_id,
            type = _type,
            task_id = task_id,
            when = when,
            where = where,
            step = step,
            event = event,
            seq = seq,
            wid = wid,
            arg = loads(arg),
            next = _next,
            pid = pid,
        )

def re_send_vector(zmq_socket, vector):
    decr_seq(vector)
    send_vector(zmq_socket, vector)


def send_vector(zmq_socket,vector, event = _SENTINEL, update=_SENTINEL):
    """
    Send a state vector other the networks

    zmq_socket a well configured socket 
    
    if event set overrides the vector event

    if update set it updates arg
    """
    assert(isinstance(vector, dict))
    assert(isinstance(vector["arg"], dict))
    if not isinstance(zmq_socket, list):
        zmq_socket = [ zmq_socket ]
    if event is not _SENTINEL:
        vector["event"] = event
    if update is not _SENTINEL:
        vector.update(update)
    try:
        if "PROPAGATE" != vector["event"]:
            incr_seq(vector)
        where = WHERE_FORMAT.format(**vector)
        when = str(float(timegm(gmtime())))
        what = WHAT_FORMAT.format(**vector)
        arg = dumps(vector['arg'])
        for sock in zmq_socket:
            sock.send("\x00".join([ where, what ,when, arg ]))
    except KeyError as k:
        raise( KeyError("malformed vector %r :<%r>" % (vector,k)))
    except Exception as e:
        logging.exception(e)


def send_event(zmq_socket,local_info, job_info, event, extra = _SENTINEL) :
    """send a multipart message from a dict on zmq_socket
    On dict
    task_id if not set is the gmt time at which message is received
    id is for workers (when there is more than one worker
    state is the state in the state machine
    (cf docs/messaging)
    """
    assert(isinstance(local_info, dict))
    assert(isinstance(job_info, dict))
    assert(isinstance(event, str))
    job_info.update(local_info)
    send_vector(zmq_socket, job_info, event, extra is _SENTINEL and extra or {})


def _magically_create_dir(filename):
    dir = path.dirname(filename)
    if not path.exists(dir):
        makedirs(dir)


class State(object):
    """
    Class that works functionnaly, but I find too complex.
    It should be based on dogpile or bearker...

    Proxy that Stores and handle the state of the job/task

    config arguments :

    - storage: contains the path to the .json containing serialized States
    - autocommit: save the task every time it is accessed for adding/removal

    Why are all properties static ? 

    .. warning::
        Not multiprocess safe for writing
        There is also a race condition that could give false positive or discretancy
        in the status.

    """

    AlreadySet = False
    def __init__(self, config = _SENTINEL,  state = {}):
        """Singleton for handling states and ensure they are persisted.
        any attribute in a state (which is a dict) that begins with __ will
        be ignored in the serialization, or if its type is not string.

        First action is to load the states from their storage.

        config: dict with following stuff : 

            * storage : relative or absolute URI to serialization store
            * autocommit (default True): will autosave the singleton 
            every time it is modified
            * readonly (default : False): makes sure you cant save the state 
            but you can modify the states in memory
            * mongo_cnx (default: mongodb://localhost:27017/state) where we store the states
            http://docs.mongodb.org/manual/reference/connection-string/


        optionally you can give a dict (that won't be checked) of states
        that will be merged with the loaded one

        mongo is used to ensure thread safe serialisation not because it is
        cool.

        """
        ### add a readonly flag for the client. 
        ### add a mechanism for ensuring one an only writer exists
        ### or use a serializer that handles conccurrency (DB or else)
        self.get = self.get_from_backend
        
        if State.AlreadySet and not config.get("readonly"):
            ### a flag may not be enough ...
            raise ValueError(
                "this class should not be instanciated twice"
            )
        try:
            if config is _SENTINEL:
                raise Exception(
                    "Please gives a configuration dict to StateCollector"
                )
            self._config = config.copy()
            ConnectionManager.configure(
                self._config.get('backend',
                    {
                        '_default_' : { 
        ### http://docs.mongodb.org/manual/reference/connection-string/
                            'uri' : 'mongodb://localhost/',
                            'db' : 'state',
                        },
                        'state': {
                            'col' : 'state',
                        }
                    }
                )
            )
            self.Backend = ConnectionManager.get_config(r"state")
                
            if config.get("readonly", False):
                def disable_save(*a,**kw):
                    raise(Exception("Not implemented in read_only mode"))
                self.get = self.get_from_backend
                self.save = self.update = self.add =  disable_save
                ### if you are the reader this function are subject
                ### to inconsistency (race condition)
                self.is_active_job = self.pending_task =  disable_save
                self._config["autocommit"] = False
            else:
                State.AlreadySet = True

        except Exception as e:
            raise(e)


        class Record(Document):
            """mongo serialisation of a state"""
            collection_name = self._config["backend"]["state"]["col"]
        
        Record.collection_name = self._config["backend"]["state"]["col"]
        Record.validate = lambda record: isinstance(record["task_id"],str) and\
            isinstance(record["job_id"],str)

        Record.con = self.Backend
        if config.get("purge_at_startup") and not config.get("readonly"):
            Record.remove()
        self.Record = Record

        self.Backend.col.create_index(
                [( 'task_id', ASCENDING),
                    ( 'job_id', ASCENDING) ],
                name = "integrity_constraint_1",
                unique = True,
                drop_dups = True,
            )
        if len(state):
            for r in state:
                try:
                    self.update(r)
                except:
                    self.add(r)

        #self.load()

    def __del__(self):
        if hasattr(self,"_config") and not self._config.get("readonly", False):
            State.AlreadySet = False

  #  @auto_save
    def delete(self, job_id = _SENTINEL, task_id = _SENTINEL):
        """Should be call once the FSM of the job/task is END"""
        if task_id is _SENTINEL:
            ### we have a record/vectgor
            task_id = job_id["task_id"]
            job_id = job_id["job_id"]

        if _SENTINEL in [ job_id, task_id ]:
            raise Exception("Cant delete a non existing job/task")
        #if job_id not in self._state or task_id not in self._state[job_id]:
        #    raise KeyError("Non existant job/task")
        # ensure that job is deleted if no more pending task
        # race condition on the following
        doc = self.get(job_id,task_id)
        doc.delete()
        del(doc)
        
        ### del(self._state[job_id][task_id])
        #if not len(self._state[job_id].values()):
        #    del(self._state[job_id])



    def get_from_backend(self, criteria , task_id = _SENTINEL):
        """dual signature fonction (yes this is bad)
        Signature 1: 
            * (job_id, task_id)  => returns the state
        Signature 2:
            * (criteria) => returns the status corresponding to the mongod query

        """
        ### really bored to put job_id and task_id
        ### dual signature
        ### 
        job_id = criteria
        if task_id is _SENTINEL and not isinstance(criteria, MutableMapping):
            raise ValueError("You can't use the dict signature with"
                    "a task_id it means nothing")
        if isinstance(criteria, MutableMapping):
            ## it is a dict \o/
            task_id = criteria["task_id"]
            criteria = criteria["job_id"]
        
        criteria = { "job_id" : criteria , "task_id" : task_id}
        ## We keep the same API as get

        res = self.Backend.col.find_one(criteria)
        if res is None:
            raise KeyError("(job_id : %r, task_id :%r)" % (job_id, task_id))
        return self.Record(res)



    def load(self):
        """explicitly load the State dict from its serializer"""
        for record in self.Backend.col.find({}):
            try:
                job_id = record["job_id"]
            except:
                ## inconsistent data, cleaning
                self.Record(record).delete()
            try:
                task_id = record["task_id"]
            except:
                ## inconsistent data, cleaning
                self.Record(record).delete()
            self._state[job_id][task_id] = self.Record(record)

    def save(self):
        """explicitly save, use only if autocommit is False"""
        return True
       #for job_id,v in self._state.items():
        #    for task_id in v:
        #        self._state[job_id][task_id].save()


    def _get(self, job_id, task_id=_SENTINEL):
        """get a state for the task for given job_id and if specified
        task_id"""
        if isinstance(job_id, MutableMapping):
            task_id = job_id["task_id"]
            job_id = job_id["job_id"]
        if task_id is _SENTINEL:
            raise ValueError("We need a task_id")
        task = self._state["job_id"]
        # hit or miss
        if not task:
            actual = self.get_from_backend(
                job_id, task_id
            )
            actual = self.Record(actual)
            self._state[job_id][task_id] = actual
        if not len(actual):
            raise KeyError("(job_id : %r, task_id :%r)" % (job_id, task_id))
        return actual


    def add(self, pushed_json):
        """used to serialised a message the first time (INIT)"""
        job_id = pushed_json["job_id"]
        task_id = pushed_json["task_id"]
        try:
            self.get(job_id, task_id)
            raise Exception(
                "Integrity error: job <%r> or task <%r> shouldn't be there" % (
                    job_id, task_id)
            )
        except KeyError:
            pass

        record = self.Record( pushed_json)
        assert(True == record.validate())
        record.save()
        assert(isinstance(pushed_json["arg"], dict))
        assert(isinstance(record["arg"], dict))
        assert(isinstance(record,self.Record))
        return record


    def update(self, pushed_json):
        """used to serialised a message any other time"""
        job_id = pushed_json["job_id"]
        task_id = pushed_json["task_id"]
        record = self.get(job_id, task_id)
        record.update(pushed_json)
        record.save()
        assert(isinstance(pushed_json["arg"], dict))
        assert(isinstance(record["arg"], dict))
        return record




