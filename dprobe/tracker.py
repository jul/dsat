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
from archery.bow import Daikyu as dict

from threading import Timer
#import zmq.green as zmq
import zmq
from zmq.utils import jsonapi
import json
from time import gmtime
from calendar import timegm
from dsat.message import parse_event
from dsat.state import ProcTracker, get_connection, _f, construct_info

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
from simplejson import loads, dumps

SENTINEL = object
#### let's 
#time_keeper = scheduler(time, sleep)
if not len(sys.argv) >= 1:
    raise( Exception("Arg"))
CONFIG, LOCAL_INFO, ID = construct_info(sys.argv, "tracker")

dictConfig(CONFIG.get("logging",{}))
log = logging.getLogger("tracker")
CONFIG.update(LOCAL_INFO)
D = log.debug

def q_madd(q, what):
    q.put(dumps(what))

def q_mget(q):
    return loads(q.get())



D("Started tracker")

CNX = get_connection(CONFIG, LOCAL_INFO)
from pprint import PrettyPrinter as PP
pp = PP(indent=4)
P = pp.pprint
P(dict(CNX))

watchdog_q = Queue()


def watcher(watchdog_q, config):
    proc_tracker = ProcTracker(config)
    delay = config.get("check_delay",3)
    now = time()
    while True: 
        busy_per_stage = q_mget(watchdog_q)
        if abs(time() - now ) > delay:
            proc_tracker.watch(busy_per_stage)
            now = time()
    
ProcWather = Process(target=watcher, args=(watchdog_q, CONFIG,))
ProcWather.start()


busy_per_stage = dict()
message_per_stage = dict()
error_per_stage = dict()
def event_listener(CNX, config):
    """Processlet responsible for routing and reacting on status change"""
    D("event listener")
    global error_per_stage, busy_per_stage, message_per_stage
    cnx = CNX


    propagated_to = dict()

    def on_failure(new):
        log.critical("KO for <%(job_id)r-%(task_id)r> %(arg)r" % new)

    def on_end( new):
        #D("JOB PROCESSED @ STEP %(step)s" % new)
        cleanUp(new)

    def cleanUp(new):
        #q_madd(timer_q,"cancel", new)
        global busy_per_stage
        busy_per_stage -= dict({ new["step"] : 1 })

    def on_begin(new):
        global busy_per_stage
        busy_per_stage += { new["step"] : 1 }


    def on_timeout(new):
        __on_error(new)

    def on_error(new):
        __on_error(new)

    def __on_error(new):
        global error_per_stage
        cleanUp(new)
        error_per_stage+={ new["step"] : 1 }
        D("OE %s" % ( _f(new) ))
        #### WHY did I put that? 
        if int(new.get("retry",0)) >= config["max_retry"]:
            D('retry %(retry)s for <%(job_id)s-%(task_id)s>' % new)
            new["event"] = "FAILURE"
            on_failure(new)
        else:
            log.critical("unhandled failure for %r" % new)
            ### could also restart failing processes here
    def on_send(new):
        pass
    
    def on_init(new):
        pass


    def on_propagate( new):
        D("just passing %s" % _f(new))
        on_propagate += { new["next"] : 1 }

    D("waiting")
    action = dict(
            INIT = False,
            BOUNCE = False,
            SEND = False,
            HAPPY_END = False,
            ACK = False,
            END = on_end,
            BEGIN = on_begin,
            PROPAGATE = on_propagate,
            ERROR = on_error,
            TIMEOUT = on_error,
            O_TIMEOUT = on_failure,
    )
    
    print("Waiting for socket to be read cf 100% CPU zmq bug")
    sleep(1)
    local_in_sox = cnx["tracker_in"]
    CLOCK_M = int(config.get("tracker_clock_every", 100))
    ignore = 0
    check_delay = config.get("check_delay", 3) * 2
    now = time()
    while True:
        new = parse_event(local_in_sox)
        if ignore == 0:
            log.debug("RCV %s" % _f(new))
            if abs(time() - now) > check_delay:
                q_madd( watchdog_q, busy_per_stage)
                log.info("zombie count %r" % busy_per_stage)
                log.info("MPS %r" % message_per_stage)
                log.info("EPS %r" % error_per_stage)
                now = time()
        ignore += 1
        ignore %= CLOCK_M
                
         
        if new["where"] != LOCAL_INFO["where"]:
           D("NOT FOR ME %s" % _f(new))
           continue
        try:

            message_per_stage += { new["step"] : 1 } 
            task_id = new["task_id"]
            job_id = new["job_id"]

            if int(new["seq"]) > config.get("max_seq", 50):
                logging.warning("<%r> was bounced <%r> times" %(new,
                    new["seq"]))
                continue
            # Wut ? event, U sure? 
            if action[new["event"]]:
                action[new["event"]](new)
        except Exception as e:
            D("MON EXC for %r is %s" % (new, e))
            log.exception( e)




event_listener(CNX, CONFIG )


