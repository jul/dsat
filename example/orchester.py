#!/usr/bin/env python
# -*- coding: utf-8 -*-

from time import time, sleep, mktime
import datetime as dt
import sched
from random import randint
import logging
from logging.config import dictConfig
import sys,os
import signal

from threading import Timer
#import zmq.green as zmq
import zmq

from zmq.utils import jsonapi
import json
from time import gmtime
from calendar import timegm
from dsat.message import send_event, State, send_vector, parse_event,\
        incr_seq, decr_seq, re_send_vector, incr_task_id
from dsat.state import ProcTracker, TimerProxy, get_connection, _f, \
        construct_info

from circus.client import CircusClient
from circus.commands import get_commands
from contextlib import contextmanager
from functools import wraps
from collections import defaultdict
from configparser import ConfigParser
from multiprocessing import Process, Queue
from repoze.lru import ExpiringLRUCache as expiringCache
from circus.util import DEFAULT_ENDPOINT_SUB, DEFAULT_ENDPOINT_DEALER

#pyzmq is string agnostic, so we ensure we use bytes
loads = jsonapi.loads
dumps = jsonapi.dumps
SENTINEL = object
#### let's 
#time_keeper = scheduler(time, sleep)
if not len(sys.argv) >= 1:
    raise( Exception("Arg"))
CONFIG, LOCAL_INFO, ID = construct_info(sys.argv, "orchester")

dictConfig(CONFIG.get("logging",{}))
log = logging.getLogger("dev")
CONFIG.update(LOCAL_INFO)
D = log.warning
import __main__ as main

D("Started %r" % main.__file__)

LOCAL_INFO = dict(
    where = CONFIG["where"],
    step = "orchester",
    pid = os.getpid(),
    wid = "0",
)

CNX = get_connection(CONFIG, LOCAL_INFO)

def event_listener(CNX, config):
    """Processlet responsible for routing and reacting on status change"""
    D("event listener")
    cnx = CNX
    monitor = cnx["tracker_out"]
    out_sign = cnx["_context"].socket(zmq.PUB)
    print( cnx)

    out_sign.connect(CONFIG["cnx"]["SUB_orchester_master"] %config)
    state_keeper = State(config["state_keeper"])
    make_task_id = lambda : str(time())

    # pyzmq aysnc startup
    print("Waiting for socket to be read cf 100% CPU zmq bug")
    sleep(1)
    poller = zmq.Poller()
    other_in = cnx["orchester_in"]
    master_sox = cnx["master"]
    master_sox.setsockopt_string(zmq.SUBSCRIBE, u"")# unicode(LOCAL_INFO["where"]))

    poller.register(master_sox, zmq.POLLIN)
    poller.register(other_in, zmq.POLLIN)
    while True:
        ready_sox = dict(poller.poll())
        D('main wait')
        new={}
        if other_in in ready_sox and ready_sox[other_in] == zmq.POLLIN:
            new = parse_event(other_in)
            #D("rcv from OTHER %s" % _f(new))
        elif master_sox in ready_sox and ready_sox[master_sox] == zmq.POLLIN:
            new = parse_event(master_sox)
            #D("rcv from MASTER %s" % _f(new))
        
        if new["where"] != LOCAL_INFO["where"]:
            
            #D("NOT FOR ME Iam %s not %s " % (new["where"], LOCAL_INFO["where"]))
            #D("*****")
            continue
        try:
        # only one message at a time can be treated not even sure I need it
            task_id = new["task_id"]
            job_id = new["job_id"]
            if "INIT" == new["event"]:
                new["task_id"] = str(task_id.isdigit() and (int(task_id)+1) or task_id)
                new["state"] = "INIT"
                new["retry"] = "0"
                new["step"] ="orchester"
                new["next"] = new["type"]
                D("initing to %s" % _f(new))
                send_vector(cnx[new["type"]], new)
                send_vector(monitor, new)

                continue

            if "PROPAGATE" == new["event"]:
                D("skipping PROPAGATE for %s" % _f(new))
                continue

            D("***** ")
        except Exception as e:
            log.exception("MON %s" % e)





### rule of thumbs every queue should be used twice and only twice

event_listener(CNX, CONFIG )


