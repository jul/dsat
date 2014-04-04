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
from time import sleep, time
from exceptions import KeyError

"""Function and class related to process control and messaging"""


__all__ = [ "incr_task_id", "_filter_dict", "parse_event", "send_vector", "send_event",  "incr_seq" ]


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
    try:
        where, step, when, arg = null_joined_string.split("\x00")
        ### I like to play with stuff
        _type, job_id, task_id, event, seq = step.split(":")
        where, step, wid, pid, _next = where.split(":")
    except Exception as e:
        raise Exception("null_joined_string (%r) parse fail reason :%r" % (null_joined_string, e))
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

def send_vector(zmq_socket,vector, event = _SENTINEL, update=_SENTINEL):
    incr_seq(vector)
    re_send_vector(zmq_socket, vector, event, update)

def re_send_vector(zmq_socket,vector, event = _SENTINEL, update=_SENTINEL):
    """
    Send a state vector other the networks

    zmq_socket a well configured socket 
    
    if event set overrides the vector event

    if update set it updates arg
    """
    assert(isinstance(vector, dict))
    assert(isinstance(vector["arg"], dict))
    if event is not _SENTINEL:
        vector["event"] = event
    if update is not _SENTINEL:
        vector.update(update)
    try:
        when = str(float(timegm(gmtime())))
        what = WHAT_FORMAT.format(**vector)
        arg = dumps(vector['arg'])
        where = WHERE_FORMAT.format(**vector)
        zmq_socket.send("\x00".join([ where, what ,when, arg ]))
    except KeyError as k:
        raise( KeyError("malformed vector %r :<%r>" % (vector,k)))
    except Exception as e:
        logging.exception(e)



def _magically_create_dir(filename):
    dir = path.dirname(filename)
    if not path.exists(dir):
        makedirs(dir)


