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
from dsat.message import send_vector, parse_event,\
        incr_seq, decr_seq, re_send_vector, incr_task_id
from dsat.state import get_connection, _f, construct_info

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

CONFIG.update(LOCAL_INFO)
import __main__ as main

log = logging.getLogger("orchester")
D = log.debug
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
    poller = zmq.Poller()
    out_sign = cnx["_context"].socket(zmq.PUB)
    out_sign.connect(CONFIG["cnx"]["SUB_orchester_master"] %config)
    other_in = cnx["orchester_in"]
    master_sox = cnx["master"]
    master_sox.setsockopt_string(zmq.SUBSCRIBE,unicode(LOCAL_INFO["where"]))
    poller.register(master_sox, zmq.POLLIN)
    poller.register(other_in, zmq.POLLIN)
    cpt=0
    now = time()
    while True:
        if not(cpt%1000):
            print "%.1f msg/sec %.2f" % (time(), 1.0*cpt/(time() - now))
            cpt = 0
            now = time()
        cpt+=1
        new={}
        ready_sox = dict(poller.poll())
        if other_in in ready_sox and ready_sox[other_in] == zmq.POLLIN:
            new = parse_event(other_in)
            D("rcv from OTHER %s" % _f(new))
        elif master_sox in ready_sox and ready_sox[master_sox] == zmq.POLLIN:
            new = parse_event(master_sox)
            D("rcv from MASTER %s" % _f(new))
        if new == {}:
            continue
        if new["where"] != LOCAL_INFO["where"]:
            log.info("NOT FOR ME Iam %s not %s " % (new["where"], LOCAL_INFO["where"]))
            #D("*****")
            continue
        try:
        # only one message at a time can be treated not even sure I need it
            task_id = new["task_id"]
            job_id = new["job_id"]
            D("RCV%r"%_f(new))
            if new["event"] in  { "INIT", "BOUNCE"}:
                re_send_vector(cnx["tracker_out"],new, "ACK", dict( pid = config["pid"])) 
                new["task_id"] = str(task_id.isdigit() and (int(task_id)+1) or task_id)
                new["state"] = "do I use that?"
                new["retry"] = "0"
                new["step"] ="orchester"
                new["event"] = "INIT"
                new["next"] = new["type"]
                D("initing to %s" % _f(new))
                D("sending to %r" % cnx[new["type"]])
                send_vector(cnx[new["next"]], new)
                #log.warning("gup %r %r" % (monitor, new))
                re_send_vector(cnx["tracker_out"],new, "SEND", dict( pid = config["pid"])) 
                #send_vector(monitor, new)
            else:
                log.warning("unknown message caught %r" % _f(new))


            if "PROPAGATE" == new["event"]:
                D("skipping PROPAGATE for %s" % _f(new))
        except Exception as e:
            log.exception("MON %s" % e)


event_listener(CNX, CONFIG )


