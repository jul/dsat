#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from functools import wraps
from calendar import timegm
from time import gmtime, sleep
from os import path, makedirs
import shutil
import logging
from collections import defaultdict, MutableMapping
from types import StringType
from simplejson import dumps
from time import sleep, time
from exceptions import KeyError
import zmq

"""Function and class related to process control and messaging"""


__all__ = [ "parse_event", "send_vector", 
    "send_event", "re_send_vector",  "incr_seq", "decr_seq", "fast_parse_event",
]


_SENTINEL = object()

def extract_vector_from_dict(a_dict):
    return { k: str(v) for k,v in a_dict.items() if k in { 
            "type",
            "channel",
            "emitter",
            "step",
            "wid",
            "where",
            "event",
            "seq",
            "next",
            "serialization",
            "pid",
        }
    }


def _filter_dict(a_dict):
    return { str(k):w for k,w in a_dict.items() if not k.startswith("_") }

"""
step: where the event has been processed
where: which machine/localization the event comes from
wid: worker id
pid; process id
next: next step

"""

WHERE_FORMAT =  "{where}:{step}:{wid}:{pid}:{next}"
"""
### TODO: DONT CODE A broken crypto
### we will resort on system (intra unix domain socket + chmod) for ensuring
### the actual impermeability of the local satelite. 
emitter: a string for identifying the emitter.

type: the channel through which the event should be sent (routing)
task_id: something to be able to distinguish tasks
event: the state of the last message (FSM)
seq : incr by one from init to happy end

"""
WHAT_FORMAT = "{emitter}:{type}:{channel}:{task_id}:{event}:{seq}"

def incr_task_id(vector):
    task_id = vector["task_id"]
    vector["task_id"]= str(task_id.isdigit() and (int(task_id)+1) or task_id)

def decr_seq(vector):
    vector["seq"]= str(( int(vector["seq"])-1 ))

def incr_seq(vector):
    vector["seq"]= str(( int(vector["seq"])+1 ))

def parse_mesg(null_joined_string):
    try:
        # MESSAGE FORMAT:
        where, envelope, serialization, payload = null_joined_string.split("\x00")
        
        emitter, _type, channel, task_id, event, seq = envelope.split(":")
        location, step, wid, pid, _next = where.split(":")

    except Exception as e:
        raise Exception("null_joined_string (%r) parse fail reason :%r" % (null_joined_string, e))
    return dict(
            type = _type,
            task_id = task_id,
            channel = channel,
            emitter = emitter,
            step = step,
            where = location,
            event = event,
            seq = seq,
            wid = wid,
            arg = payload,
            next = _next,
            serialization = str(serialization),
            pid = pid,
        )
    

def fast_parse_event(zmq_socket):
    """Takes a well configure socket and returns the state.
        # MESSAGE FORMAT:
        {where}:{step}:{wid}:{pid}:{next}\0{emitter}:{type}:{channel}:{task_id}:{event}:{seq}\0serialization\0payload
    The returned vector contains:
        * task_id = something I intend to use in the future to put a series of CSV 
            NEXT values for auto routing from source
        * where = on which server
        * step = the worker's circus name that is actually suppose to consume
        * next the next step to send (info)
        * wid: circus worker ID
        * seq: message sequence
        * arg: the unserialized arguments
        * pid
        * serialization: the format in which arg was serialized
        * event the event that is sent

    """
    s = zmq_socket
    recv=s.recv
    null_joined_string = recv()
    return parse_mesg(null_joined_string)

identity = lambda a: a

def get_payload(message):
    from .state import serializer_for
    return serializer_for(message["serialization"])(message["arg"])

def parse_event(zmq_socket):
    """
        Commodity variant if you need to access the args sent in the message
    """
    from .state import serializer_for
    to_return = fast_parse_event(zmq_socket)
    to_return["arg"] = serializer_for(to_return["serialization"])(to_return["arg"])
    
    return to_return

parse_vector = parse_event
fast_parse_vector = fast_parse_event


def send_vector(zmq_socket, vector, event = _SENTINEL, update=_SENTINEL):
    vector["seq"] = vector.get("seq", "0")

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
    topic = ""
    if event is not _SENTINEL:
        vector["event"] = event
    if update is not _SENTINEL:
        vector.update(update)
    try:

        what = WHAT_FORMAT.format(**vector)
        where = WHERE_FORMAT.format(**vector)
        vector["serialization"] = str(vector["serialization"])
   #print "MSG IS %s" % "\x00".join([ where, what , vector["serialization"], vector["arg"]])
        send = zmq_socket.send
        send("\x00".join([ where, what , vector["serialization"], vector["arg"]]))

    except KeyError as k:
        logging.error("malformed vector %r : <%r>" % (vector, k))
        raise( KeyError("malformed vector %r :<%r>" % vector,k))
    except Exception as e:
        logging.error("MSG is "  + ",".join(map(repr,[ where, what , vector["serialization"], vector["arg"]])))
        raise(e)



def _magically_create_dir(filename):
    dir = path.dirname(filename)
    if not path.exists(dir):
        makedirs(dir)


