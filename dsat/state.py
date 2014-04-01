#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
"""
Utilities related to state machine handling of the asynchrone FSM



"""
from time import sleep, mktime
import sys
if sys.version_info < (3, 2) and "linux" in sys.platform:
    # thx to vstinner time is now monotonic on new python versions
    from .linux_mtime import m_time as time
else:
    from time import time

import datetime as dt
import sched
from random import randint
from os import path
import logging
from logging.config import dictConfig
import sys,os
import signal
import socket

from threading import Timer
import zmq.green as zmq
#import zmq
from zmq.utils import jsonapi
import json
from time import gmtime
from calendar import timegm
from .message import send_event, State, send_vector, parse_event, re_send_vector
from circus.client import CircusClient
from circus.commands import get_commands
from contextlib import contextmanager
from functools import wraps
from collections import defaultdict, MutableMapping
from configparser import ConfigParser
from multiprocessing import Process, Queue, Lock

from circus.util import DEFAULT_ENDPOINT_SUB, DEFAULT_ENDPOINT_DEALER

#pyzmq is string agnostic, so we ensure we use bytes

SENTINEL = object

__all__ = [ "_f", "get_connection", "ProcTracker", "TimerProxy" ]

context = zmq.Context()
_f = lambda d: ":".join([ str(d.get(k,"NaV")) for k in [ "seq", "step","next",
        "state","job_id", "task_id","retry","event"] ])

def get_connection(CONFIG, LOCAL_INFO):
    """list of available connections to neighborhoods
    given the actual position.

    it uses the CONFIG to get the connections topology
    and LOCAL_INFO to get the actual step

    Not meant to be used as is

    Parsing circus.ini to get the ZMQ topology.
    """
    cnx_list = defaultdict(dict,{})
    cnx_list["_context"] = context
    cnx_list["next"] ={}
    cnx_list["previous"] = set()
    CONFIG.update(LOCAL_INFO)
    here = CONFIG["step"]
    print("building cnx list for step %(step)s" % CONFIG)
    interesting_link = [
        link for link in CONFIG["cnx"] if ( "any" not in link and \
            here in link.split("_") )
    ]
    rec_scheme=dict(
        PUB="SUB", SUB="PUB",
        PUSH="PULL", PULL="PUSH")
    cfg = ConfigParser()
    # circus prefix for the process name
    prefix_proc= "watcher:"
    offset_in_st = len(prefix_proc)
    assert(path.exists(CONFIG.get("circus_cfg", "circus.ini")))
    cfg.read(CONFIG.get("circus_cfg", "circus.ini"))
    already_bound = dict()
    # singleton in circus means fixed points
    singleton = set([ proc[offset_in_st:] for proc in cfg \
            if proc.startswith(prefix_proc) and cfg[proc].get("singleton") ])
    for link in interesting_link:
        ## conf is PUSH_pusher_puller
        scheme , src, dst = link.split("_")
        if "tracker" in {src, dst}:
            continue 
        #orchestrer is the init point of this circus setup
        if src == here or ( "orchester" == here and "orchester" == src ):
            scheme = getattr(zmq,scheme)
            step = dst
            is_next = True
            cnx = cnx_list[step] = context.socket(scheme)
            if not cnx_list.get("next", {}).get(dst):
                cnx_list["next"][dst] = cnx
        else:
            step = src
            scheme= rec_scheme[scheme]
            scheme = getattr(zmq,scheme)
            cnx = cnx_list[step] = context.socket(scheme)
            is_next = False
            print "previous is %r" % cnx
            cnx_list["previous"] |= { cnx }
            # singleton bind to their end, moving parts connect for outgoing links
            # opposite for incoming link
            # EXCEPT master to orchester
        address = CONFIG["cnx"][link] % CONFIG

        if ( "orchester" == src and "orchester"==here and "master"!= dst) or \
            "orchester" != here and here in singleton:
            if address not in already_bound:
                # believe me you want this to troubleshoot
                cnx.bind(address)
                print("##[%s] %s // binding %r / %r on %r / %r" % (step, here, scheme, step,address, cnx))
                already_bound[address] = step
            else:
                cnx = cnx_list[step] = cnx_list[already_bound[address]] 
            is_singleton = True
        else:
            print("%s CONNECT on %s;%s because single" % (here, link, address))
            if address not in already_bound: 
                cnx.connect(address)
                print("[%s] cnxing %r / %r on %r/%r" % (step, scheme, step,address, cnx))
                already_bound[address] = step
            else:
                cnx = cnx_list[step] = cnx_list[already_bound[address]]
            is_singleton = False
        if not is_next:
            cnx_list["previous"] = {  cnx, } 
    if "orchester" == here:
        cnx = cnx_list["orchester_in"] = context.socket(zmq.PULL)
        cnx.bind(CONFIG["cnx"]["PUSH_any_orchester"] % CONFIG)
        print "[%s] BIND with  on o_in w PULL %s" % (
            here, CONFIG["cnx"]["PUSH_any_orchester"] % CONFIG)

    else:
        cnx = cnx_list["orchester_out"] = context.socket(zmq.PUSH)
        cnx.connect(CONFIG["cnx"]["PUSH_any_orchester"] % CONFIG)
        print "[%s] orchester CNX with  on o_in w PUSH %s" % (
            here,  CONFIG["cnx"]["PUSH_any_orchester"] % CONFIG)
    if "tracker" == here:
        cnx = cnx_list["tracker_in"] = context.socket(zmq.PULL)
        cnx.bind(CONFIG["cnx"]["PUSH_any_tracker"] % CONFIG)
        cnx = cnx_list["tracker_out"] = context.socket(zmq.PUB)
        cnx.bind(CONFIG["cnx"]["PUB_tracker_any"]%CONFIG)
    else:
        cnx = cnx_list["tracker_out"] = context.socket(zmq.PUSH)
        cnx.connect(CONFIG["cnx"]["PUSH_any_tracker"] % CONFIG)
        #cnx = cnx_list["tracker_in"] = context.socket(zmq.SUB)
        #cnx.connect(CONFIG["cnx"]["PUB_tracker_any"]%CONFIG)
        # tracker is the process dedicated to get the statuses.  
        print "tracker in is %s " % ( CONFIG["cnx"]["PUSH_any_tracker"] % CONFIG)
    return dict(cnx_list)

    # will be set to def() pass one day, this is my creative way of setting debug levels
D = logging.warning

class ProcTracker(object):
    """
    Tracks process number and spawn new process given constraints
    Alpha version I hate using mongodb.
    Next version aims at being far more simpler.
    TOFIX: I think I have a leaky abstraction here. 

    For StateWrapper I will probably use a repoze.lru that should suits 
    small scale state handling (with 100K entries should suit in memory)
    """
    def __init__(self, CONFIG):
        """
        CONFIG : a dict that only needs to countains 
        { "circus_cfg" : path_to_circus_ini_file }

        """
        cfg = ConfigParser()
        prefix_proc= "watcher:"
        offset_in_st = len(prefix_proc)
        cfg.read(CONFIG.get("circus_cfg", "circus.ini"))
        self._alive = { i[offset_in_st:] : int(cfg[i]["numprocesses"]) for i in cfg
            if i.startswith(prefix_proc) and not cfg[i].get("singleton") 
        }

        ProcTracker.circus = CircusClient(
            endpoint = cfg["circus"].get("endpoint", DEFAULT_ENDPOINT_DEALER),
            timeout = CONFIG.get("circus_ctl_timeout", 10),
        )
        self._config = CONFIG
        state_config = CONFIG.get("state_keeper")
        state_config["readonly"] = True
        self.state  = State(state_config)



    def _running_process(self, proc):
        return self.state.Backend.col.find(
                {   "step" : proc,
                ### You really don't want a perfect memory. 
                    "when" : { "$gt" :str(time() - 
                        self._config.get('relaxing_mem',7200))
                    },
                    "state": "PROCESSING",  }).count()
        
    def _process_alive(self,proc):
        return self._circus_cmd("numprocesses", proc)
        


    def circus_incr(self, process):
        """
        not to be used directly in fact
        """
        #logging.debug("incr %r" % process)
        try:

            self._circus_cmd("incr", process)
        except Exception as e:
            ProcTracker._alive[process] -= 1
            #D("ARG")
            #logging.exception(e)


    def circus_decr(self, process): 
        """
        not to be used directly in fact
        """
        #logging.debug("incr %r" % process)
        try:
            self._circus_cmd("decr", process)
        except Exception as e:
            ProcTracker._alive[process] += 1
            D("ARG")
            logging.exception(e)

    def _circus_cmd(self, cmd, process):
        #logging.debug("for <%s> calling <%s>  " % (process, cmd))
        return ProcTracker.circus.call( dict(
            command= cmd,
            properties = dict(
                name = process
            )
        )).get(cmd)

    def watch(self, process):
        """
        Process handling here.

        The more I look at it, the more I think it could be a function
        """
        if not process in self._alive:
            D("%r ignored" % process)
            return 
        else:
            nb_running = self._running_process(process) 
            nb_alive = self._process_alive(process)
            D("avsr%r %s vs %r" % ( process, nb_alive , nb_running))
            if nb_alive  - nb_running < self._config.get("minimal_worker_limit",5) :
                self.circus_incr(process)
            if nb_alive - nb_running > self._config.get("max_useless_worker", 10) :
                self.circus_decr(process)
            

class TimerProxy(object):
    """Timer proxy, wrapper around a minimal scheduler that keeps track
    of the ongoing tasks"""
    def __init__(self, *a, **kw):
        self._active_task = {}
        #self.D = logging.debug

    def add_event_in(self, delay, id, what, *arg):
        """in 

        * delay seconds
        * given the unique id *id*
        * schedule fonction *what*
        * given the positional args *arg*
        """

        #new scheduled event cancel all previsous events
        res = self.cancel_event(id)
        #self.D("Timer(what, When args <%r> <%r> <%r>)" % (delay, what, arg))
        event = Timer(delay ,what, arg)
        self._active_task[id] = event
        #self.D("added %r for id=%r" % (event, id)) 
        event.start()
        return res

    def cancel_event(self, id):
        """ cancel task by id"""
        event = self._active_task.get(id)
        if event:
            event.cancel()
            del(event)
            del(self._active_task[id])
            return True
        return False



def router(argv, name, **kw):
    """ argv are command line arguments  expecting the following ; 

    * global configuration
    * local_configuration
    * if exists the worker id

    func is the function wrapped:
    * it must be in the PUSH_step1_step2 connexions
    * it must have in the circus [watcher:....] section
    the name of the watcher should be the same as the func name and the cnx
    """
    kw['is_router'] = True
    state_wrapper(argv, name, **kw)

def construct_info(argv, here):
    CONFIG = {}
    with open(argv[1]) as conf:
        CONFIG = json.load(conf)
        with open(argv[2]) as local_conf:
            CONFIG.update(json.load(local_conf))

    ID = ( len(argv) >=4 ) and argv[3] or "0"
    CONFIG["logging"].update({
        "formatters": {
            "verbose": {
                "format": "%(asctime)s [%(levelname)s] [" +here+":"+ID+ \
                        ":%(module)s:%(lineno)d] %(process)d %(message)s"
            },
            "simple": {
                "format": "[%(levelname)s] ["+here+":"+ID+\
                        ":%(module)s:l%(lineno)d] %(process)d %(message)s"
            }
        }
    })
    host_name = socket.gethostname()
    LOCAL_INFO = dict(
        where = CONFIG.get("where",
            socket.gethostbyaddr(host_name)[0]
        ),
        wid = ID,
        id=ID,
        step = here,
        ip= socket.gethostbyname(socket.gethostname()),
        ext_ip = socket.gethostbyname(socket.gethostname()),
        pid = os.getpid(),
    )
    if "where" in CONFIG:
        assert(LOCAL_INFO["were"] == CONFIG["where"])
    return CONFIG, LOCAL_INFO, ID

def state_wrapper(argv, func_or_name, **kw):
    """ argv are command line arguments  expecting the following ; 

    * global configuration
    * local_configuration
    * if exists the worker id

    func is the function wrapped:
    * it must be in the PUSH_step1_step2 connexions
    * it must have in the circus [watcher:....] section
    the name of the watcher should be the same as the func name and the cnx
    """

    
    func = func_or_name
    here = func.__name__ if hasattr(func_or_name, "__name__") else func_or_name
    print("wrapping %r" % here)
    is_router = kw.get("is_router")

    ## Gzzz I hate this bleow
    
    CONFIG, LOCAL_INFO, ID = construct_info(argv, here)
    if "where" in CONFIG:
        assert(LOCAL_INFO["were"] == CONFIG["where"])

    dictConfig(CONFIG["logging"])
    log = logging.getLogger("dev")

    def nop(*a): pass
    D = CONFIG.get("debug", True) and ( 
        lambda msg: log.debug("W:%s %s" % (ID, msg))) or nop

    _SENTINTEL = object()
    cnx = get_connection(CONFIG, LOCAL_INFO)
    LOCAL_INFO['next'] = cnx.get('next') and _SENTINTEL or "END"
    
    if "where" in CONFIG:
        assert(LOCAL_INFO["where"] == CONFIG["where"])

    bouncer = bounce_cfg = bouncer_index = None
    bouncer_dict = dict()
    if kw.get("bounce_to"):
        bouncer = True
        bounce_cfg = CONFIG["bounce_to"]
        bouncer_list = [k for k in  bounce_cfg.keys() if k in kw["bounce_to"]]
        D("boucner %r" % kw["bounce_to"])
        bounce_index=dict()
        already_opened = dict()
        for type_on_orchester in bouncer_list:
            bouncer_dict[type_on_orchester] = [ ]
            for cfg in CONFIG["bounce_to"][type_on_orchester]:
                bounce_sox_address = \
                    CONFIG["cnx"]["PUSH_any_orchester"] % \
                        dict( ext_ip = socket.gethostbyname(cfg['where']),
                    )
                bouncer_sox = already_opened.get(bounce_sox_address)

                if not bouncer_sox:
                    scheme = zmq.PUSH
                    bouncer_sox = context.socket(scheme)
                    bouncer_sox.connect(bounce_sox_address)
                bouncer_dict[type_on_orchester] += [ bouncer_sox ]

                bounce_index[type_on_orchester] = 0
        del(already_opened) 
    #from pprint import PrettyPrinter as PP
    #P = PP(indent=4).pprint
    #P(dict(cnx))
    #P(LOCAL_INFO)
    #print P(cnx)
    previous_cnx = cnx["previous"]
    assert(len( previous_cnx ) == 1)
    assert(isinstance(previous_cnx, set))
    previous_cnx = previous_cnx.pop()
    next_cnx = False
    if "END" != LOCAL_INFO['next']:
        next_cnx = cnx["next"]

    status  = cnx["tracker_out"]

    D("starting %s"%here)
    _d_bind=0

    while True:
        #print previous_cnx
        #print previous_cnx
        event = parse_event(previous_cnx)
        D("RECV %r" % event)
        assert(isinstance(event["arg"], dict))
        #D("treating %s" % _f(event))
        task_id = event["task_id"]
        job_id = event["job_id"]
        send_vector(status, event, "ACK")
        probe_res = {}

        event.update(LOCAL_INFO)
        max_retry = CONFIG.get("max_retry", 1) if next_cnx else 1
        retry = 1
        ok =  False
        while not ok and retry <= max_retry:
            try:
                if not is_router:
                    retry +=1 
                    send_vector(status, event,"START")
                    D("START")
                    event["arg"].update( { ( "_%s" % k) :v for \
                         k,v in event.items() if "arg" != k } )
                    probe_res = func(cnx,event["arg"])
                    event["arg"] = isinstance(probe_res, dict) and probe_res or {}

                    event["arg"]["_retry"] = retry

                    send_vector(status, event ,"END_OF_JOB" )
                
                D("cnx['next'] is %r " % cnx["next"]) 
                for step, to_send in  cnx.get('next', {}).items():
                    D("step is %r " % step)
                    if kw.get(
                            "cond_for", dict()
                        ).get(
                            step,lambda ev:True)(event):
                        event["next"] =  step
                        send_vector(to_send, event ,"SEND" )
                        D("sent to step %r" %step) 
                    else: 
                        D("not sent to step %r" %step) 
                event["next"] = "tracker"
                send_vector(status, event , "next" in cnx and "END" or "HAPPY_END" )
                if not _d_bind - 1:
                   _d_time_now = time()
                if bouncer:
                    cond = lambda ev:True
                    for channel in bouncer_dict:
                        if kw.get("cond_for"):
                            cond = kw["cond_for"].get(channel, cond)
                        if cond(event):
                            D("bouncing on channel %r" % channel)
                            current_bouncer = bouncer_dict[channel][\
                                bounce_index[channel]
                            ]
                            current_cfg = bounce_cfg[channel][bounce_index[channel]]
                            _here = current_cfg["where"]
                            bevent = event
                            bevent["seq"] = 0
                            bevent["step"] = "orchester"
                            bevent["next"] = channel
                            bevent.update(current_cfg)
                            bevent["where"] = _here
                            bevent["state"]="INIT"
                            if not job_id.endswith(_here):
                                bevent["job_id"] += "_" + _here
                            bevent["arg"]["_new_type"] = channel
                            bevent["arg"]["_from_where"] = event["where"]
                            send_vector(bouncer_dict[channel], bevent, "INIT")
                            D("bounced  %s" % _f(bevent))
                            bounce_index[channel] += 1
                            bounce_index[channel] %= len(bounce_cfg[channel])
                            _d_bind += 1
                        if not(_d_bind%400):
                            _d_inter = time() - _d_time_now
                            print "%d in %f = %f ev/s" % (_d_bind,_d_inter,1.0 * _d_bind/_d_inter) 
                        if not((_d_bind%200)+1):
                            _d_inter = time() - _d_time_now
                            _d_time_now += _d_inter / 2
                            _d_bind/=2
                ok = True
            except Exception as e:
                log.exception("Processing failed @%s %s" % (here,e))
                send_vector(status, event,"ERROR", {
                    "_retry" : retry,
                "_error_type" : str(type(e)) ,
                "_status": str(e)
            })
            sleep(CONFIG.get("sleep_after_retry", 2))





