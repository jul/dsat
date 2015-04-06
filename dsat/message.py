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
import zmq.green as zmq
from zmq.eventloop import ioloop, zmqstream

"""Function and class related to process control and messaging"""


__all__ = [ "parse_event", "send_vector", 
    "send_event", "re_send_vector",  "incr_seq", "decr_seq", "fast_parse_event",
]


_SENTINEL = object()
TEMPLATE = dict(
    type= "",
    seq = 0,
    task_id = '0',
    event= "INIT",
    next="",
    where = "localhost",
    emitter = "",
    channel = "",
    wid = '',
    step = '',
    pid = '1',
    serialization = 'str',
)
def serializer_for(module_name, primitive="loads"):
    assert module_name not in set([ None, "None" ])
    if module_name  == "str":
        return str
    ser_module = __import__(module_name, globals(), locals(), [primitive, ], -1)
    return getattr(ser_module, primitive)

def extract_vector_from_dict(a_dict):
    return { k: a_dict.get(k, TEMPLATE[k]) for k in { 
            "type",
            "channel",
            "emitter",
            "step",
            "task_id",
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
    
def turbiner(*arg, **option):
    zmq_pool = arg[:-1]
    handler = arg[-1]
    faster= option.get("faster")
    def decapsulating_handler(msg_batch):
        for msg in msg_batch:
            try:
                event = parse_mesg(msg)
                if not faster:
                    event["arg"] = serializer_for(event["serialization"])(event["arg"])
                handler(event)

            except Exception as e:
                logging.exception(e)
                logging.critical(e)
    for zmq_socket in zmq_pool:
        ss_stream = zmqstream.ZMQStream(zmq_socket)
        ss_stream.on_recv(decapsulating_handler)

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
    return serializer_for(message["serialization"])(message["arg"])

def parse_event(zmq_socket):
    """
        Commodity variant if you need to access the args sent in the message
    """
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
        if not isinstance(vector["arg"], str):
            vector["arg"] = serializer_for(vector["serialization"], "dumps")(vector["arg"])

   #print "MSG IS %s" % "\x00".join([ where, what , vector["serialization"], vector["arg"]])
        send = zmq_socket.send
        send("\x00".join([ where, what , vector["serialization"], vector["arg"]]))

    except KeyError as k:
        logging.error("malformed vector %r : <%r>" % (vector, k))
        raise( KeyError("malformed vector %r :<%r>" % vector,k))
        logging.exception(e)
    except Exception as e:
        logging.error("MSG is "  + ",".join(map(repr,[ where, what , vector["serialization"], vector["arg"]])))
        logging.error("WHAT:%r" % what)
        logging.error("WHERE:%r" % where)
        logging.error("SERIA:%(serialization)r" % vector)
        logging.error("ARG:%(arg)r" % vector)

        logging.exception(e)
        raise(e)



def _magically_create_dir(filename):
    dir = path.dirname(filename)
    if not path.exists(dir):
        makedirs(dir)


