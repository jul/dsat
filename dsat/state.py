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
#import zmq.green as zmq
import zmq
from zmq.utils import jsonapi
import json
from time import gmtime
from calendar import timegm
from .message import send_vector, fast_parse_vector, re_send_vector,  incr_task_id
from circus.client import CircusClient
from circus.commands import get_commands
from contextlib import contextmanager
from functools import wraps
from collections import defaultdict, MutableMapping
from configparser import ConfigParser
from multiprocessing import Process, Queue, Lock

from circus.util import DEFAULT_ENDPOINT_SUB, DEFAULT_ENDPOINT_DEALER

#pyzmq is string agnostic, so we ensure we use bytes

_SENTINEL = object

__all__ = [ "_f", "get_connection", "ProcTracker", "TimerProxy", "Connector",
"serializer_for" ]

context = zmq.Context()
_f = lambda d: ":".join([ str(d.get(k,"NaV")) for k in [ "pid", "seq", "step","next",
        "serialization", "event","retry"] ])


identity = lambda a:a

def double_load_json(file1, file2):
    CONFIG = {}
    with open(file1) as conf:
        CONFIG = json.load(conf)
        with open(file2) as local_conf:
            CONFIG.update(json.load(local_conf))
    return CONFIG

def serializer_for(module_name, primitive="loads"):
    assert module_name not in set([ None, "None" ])
    if module_name  == "str":
        return str
    ser_module = __import__(module_name, globals(), locals(), [primitive, ], -1)
    return getattr(ser_module, primitive)

def handle_function_call(self,  payload, vector, **kw):
    """calls function with everything and handle magically the
    recasting of messages"""
    payload = serializer_for(
                                vector["serialization"], "loads"
                            )(
                                vector["arg"]
                            )
    res = self.func(self , payload , vector)
    res = serializer_for(
            vector["serialization"], "dumps"
        )(
            res
        )
    vector["arg"] = res
    assert(isinstance(vector["arg"], str))

class Connector(object):
    """
    latin co-nexion
        putting nodes (edges) in relationship (by the mean of vertices)

    Class connecting a process to its neighbours
        also provides the access to facility such as:
            * local config
            * preconfigured logger
            * serialization handling
            * out of band signaling (orcherster)
            * process management (tracker)
            * access to a point of presence for external satellite
                (it may be orcherster or master, I forgot)
            * conditionnal vector muxing on output

        """
    @staticmethod
    def construct_info_from_cli( func_or_name, *argv):
        ### TODO : argv[0] =~ here why bother?
        if not argv:
            argv=sys.argv
        if len(argv) == 1:
            argv = argv[0]
        CONFIG = double_load_json(*argv[1:3])
        ID = ( len(argv) >=4 ) and argv[3] or "0"
        func = func_or_name
        here = func.__name__ if hasattr(func_or_name, "__name__") else func_or_name
        return Connector.construct_info(here, CONFIG, ID)
        ## a little glitch in my code
    @staticmethod
    def construct_info( here,CONFIG , ID):
        if len( {"global_config", "local_config" } & set(CONFIG.keys())) == 2:
            CONFIG = double_load_json(
                CONFIG["global_config"],CONFIG["local_config"]
            )
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
            assert(LOCAL_INFO["where"] == CONFIG["where"])
        return here, CONFIG, LOCAL_INFO, ID


    def __init__(self, func_or_name, *argv, **option):
        """
        * the name of the function should be the name of the circus watcher 
          AND the name of the step. That is how I join the information from 
          the edges and the vertices.

        * edges are named either by their true name, or give me the
        function to wrap and I will find it
        * vertex: It is the file containing connection matrices and local_info
        * edge (circus.ini) describing how each edges are launched
            * if singleton is seen, I consider it is a router, else a worker
        * option :
            * is_router = True: 
                no need to parse vector it is a ventilator (cf ZMQ guide)
            * serialization: name of a module that has dumps/loads with wich 
            to freeze / thaw data
                * nothing: vector is a string
                * json : advice use simplejson it is way faster
                * pickle/marshall: ahahha (use at your own risks)
                    nothing guaranties that all workers are using the same
                    version of python on a distributed system .... and 
                    marshall format varies per python version
                * dirty (to reimplement): a way to pass lambda function by capturing
                    the code and directly transmit it. Has a safeguard with py version
                * local_info: a dict to amend global_info (like DB settings, passwords
                THAT SHOULD NOT BE STORED IN CLEAR TEXT)
            * local_info: optional update to the local config
            * bounce_to BLACK MAGIC 
                not documented because I intend to make $$$ with it
        func is the function wrapped:
            * it must be in the PUSH_step1_step2 connexions
            * it must have in the circus [watcher:....] section
        the name of the watcher should be the same as the func name and the cnx
        

                
            #TODO find all the option.get("...") in this page and document
        """
        construct = Connector.construct_info_from_cli
        _arg = len(argv) and argv or sys.argv
        self.here, self.vertex, self.local_info, self.worker_id = \
            Connector.construct_info_from_cli(func_or_name,*_arg) 
        self.func = func_or_name
        
        self.option = option
        self.local_info.update(option.get("local_info", {}))
        self.config = self.vertex
        self.is_router = option.get("is_router")
        self.cnx =  Connector.get_connection(self.vertex , self.local_info)
        dictConfig(self.vertex["logging"])
        self.log = logging.getLogger(
            self.here in self.vertex["logging"]["loggers"] and self.here or "dev"
        )
        self.local_info['next'] = self.cnx.get('next') and "unset" or "TERMINUS"
        self.log.info(("wrapping %(here)s" % self.__dict__) + "[%(wid)s]" % self.local_info )
    
    def turbine(self):
        """Verb: turbiner : slang french for working hard
            turbine == imperative format
            n.f: a stuff that spins like hell in an engineering system
        Does basically the wait and process

        """
        cnx = self.cnx
        if "where" in self.vertex:
            assert(self.local_info["where"] == self.vertex["where"])
        
        assert self.is_router or not(isinstance(self.func, str)), \
            "Ouch wont work"
        
        
        def nop(*a): pass
        
        D = self.config.get("debug", True) and self.log.debug or nop

        D("waiting vector %(here)s" % self.__dict__)
        bouncer = bounce_cfg = bouncer_index = None
        bouncer_dict = dict()

        previous_cnx = self.cnx["previous"]
        assert(len( previous_cnx ) == 1)
        assert(isinstance(previous_cnx, set))
        previous_cnx = previous_cnx.pop()
        next_cnx = False
        if "END" != self.local_info['next']:
            next_cnx = cnx.get("next")

        status  = self.cnx["tracker_out"]

        while True:
            
            vector = fast_parse_vector(previous_cnx)
            D("RECV %r" % vector)

            
            #D("treating %s" % _f(vector))
            task_id = vector["task_id"]
            probe_res = {}

            vector.update(self.local_info)
            re_send_vector(status, vector, "ACK")
            
            if not next_cnx:
                vector["next"] = "TERMINUS"
            
            max_retry = self.option.get("max_retry", 1) if next_cnx else 1
            retry = 1
            ok =  False
            while not ok and retry <= max_retry:
                try:
                    if not self.is_router:
                        retry +=1 
                        #re_send_vector(status, vector, "BEGIN")
                        D("BEGIN")
                        handle_function_call(self, vector["arg"], vector)
                        #assert(isinstance(vector["arg"], str))
                        #re_send_vector(status, vector, "END")

                    D("cnx['next'] is %r " % self.cnx.get("next", "not set")) 
                    is_last = True
                    if is_last:
                        re_send_vector(status, vector, "HAPPY_END")
                        for step, to_send in  cnx.get('next', {}).items():
                            D("next step is %r " % step)
                            assert(isinstance(step,str) or isinstance(step, unicode))
                            vector["next"] =  step
                            send_vector(to_send, vector ,"SEND")
                            #re_send_vector(status, vector ,"SEND")
                            is_last = False
                            D("sent to step %r" %step) 
                        
                        if is_last:
                            re_send_vector(status, vector, "HAPPY_END")
                        ok = True
                except Exception as e:
                    self.log.error("@%s %s" % (self.here,e))
                    self.log.exception(e)
                    sleep(self.vertex.get("sleep_after_retry", 2))
                    re_send_vector(status, vector,"ERROR")


    @staticmethod
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
        cnx_list["next"] =defaultdict(dict,{})
        cnx_list["previous"] = set()
        CONFIG.update(LOCAL_INFO)
        here = CONFIG["step"]
        CONFIG['here']=here
        print("[%(here)s] building cnx list" % CONFIG)
        interesting_link = [
            link for link in CONFIG["cnx"] if ( "any" not in link and \
                here in link.split("_") )
        ]
        rec_scheme=dict(
            PUB="SUB", SUB="PUB",
            PUSH="PULL", PULL="PUSH",
            REP = "REQ", REQ="REP",
            PAIR = "PAIR",
        )
        number_to_scheme = { getattr(zmq, s): s for s in rec_scheme.keys() +
rec_scheme.values() }
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
            #if here == "master":
            #    print "[%s] special case" % link
            #    continue
            if ( src == here ):
                print "[%s] OUT connection %s/%s=>%s" % (here,scheme, src, dst)
                scheme = getattr(zmq,scheme)
                is_next = True
                cnx = cnx_list[dst] = context.socket(scheme)
                if not cnx_list.get("next", {}).get(dst):
                    print "[%s] %s/%s=>%s in NEXT cnx" % (here,number_to_scheme[scheme], src, dst)
                    cnx_list["next"][dst] = cnx
            else:
                scheme= rec_scheme[scheme]
                scheme = getattr(zmq,scheme)
                print "[%s] IN connection %s/%s=>%s (previous)" % (here,number_to_scheme[scheme],
src, dst)
                ## dont call your step next ... think of reserved keyword ...
                cnx = cnx_list[here == "master" and src or dst] = context.socket(scheme)
                is_next = False
                cnx_list["previous"] |= { cnx }
                # singleton bind to their end, moving parts connect for outgoing links
                # opposite for incoming link
                # EXCEPT master to orchester
            address = CONFIG["cnx"][link] % CONFIG

            if ( "orchester" == src and "orchester"==here and "master"!= dst
and dst not in singleton) or \
                ("orchester" != here) and (here in singleton and here ==dst ) \
or (here, dst) == ('tracker', 'consumer'):

                print  "%s (%s)" % (here, type(here))
                print  "%s (%s)" % ("tracker", type("tracker"))
                print  "%s (%s)" % (dst, type(dst))
                
                if address not in already_bound:
                    # believe me you want this to troubleshoot
                    print("[%s] BINDING %s/%s=>%s (%s) / %r / <%r>" %
                        (here, number_to_scheme[scheme], src, dst, link, address, cnx))
                    cnx.bind(address)
                    already_bound[address] =  here == src and dst or src 
                else:
                    print("[%s] ALREADY %s/%s=>%s (%s=%s?) / %r" %
                        (here, scheme, src, dst,  here, address, cnx))
                    cnx = cnx_list[dst] = cnx_list[already_bound[address]] 
                is_singleton = True
            else:
                print "(%s, %s) != (tracker, consumer) " % (here, dst)
                print("[%s] %s/%s=>%s CONNECT on %s;%s because single %r" % \
(here,number_to_scheme[scheme], src, dst, link, address, cnx))
                if address not in already_bound: 
                    cnx.connect(address)
                    print("[%s] %s/%s=>%s cnxing %r / <%r>" % (here
, number_to_scheme[scheme], src,
dst, address, cnx))
                    already_bound[address] = [ src, dst ][here == src ]
                else:
                    cnx = cnx_list[dst] = cnx_list[already_bound[address]]
                is_singleton = False
            if scheme in { zmq.SUB, 'SUB' }:
                print "[%s] This is not optimal (SUB everything)" % link
                cnx.setsockopt(zmq.SUBSCRIBE, "")
            if not is_next:
                cnx_list["previous"] = {  cnx, } 
        #if "master" == here:
         #    cnx_list["previous"] |= { cnx_list["producer"] }
            #cnx = context.socket(zmq.SUB)
            ## hint : don't declare consumer as singleton in edge
            ## it is a convenience for stuff that should not be in edge file
            #cnx.bind(CONFIG["cnx"]["PUB_consumer_master"] % CONFIG )
            #print "[MASTER] BIND TO THE WORLD ON %s %r" % (
            #    CONFIG["cnx"]["PUB_consumer_master"] %CONFIG, cnx)
            #print "[MASTER] SUBSCRIBING to %(where)s" % (CONFIG)
            #cnx.setsockopt(zmq.SUBSCRIBE, "")
            #cnx_list["previous"] = { cnx["master"] }
            #cnx_list["master"] = cnx["master"]
        if ( "orchester" == here ):
            cnx = context.socket(zmq.PULL)
            cnx.bind(CONFIG["cnx"]["PUSH_producer_orchester"] % CONFIG)
            cnx_list["orchester_in"] = cnx
            print "[%s] %s=>%s PUSH_producer_orchester BIND with  %s // %r" % (
                here, "any", "orchester", CONFIG["cnx"]["PUSH_producer_orchester"] % CONFIG, cnx)

        else:
            if (here != "master"):
                cnx = context.socket(zmq.PUSH)
                cnx.connect(CONFIG["cnx"]["PUSH_producer_orchester"] % CONFIG)
                cnx_list["orchester_out"] = cnx
                print "[%s] %s=>%s PUSH_producer_orchester CNX %s" % (
                    here, "any", "??",  CONFIG["cnx"]["PUSH_producer_orchester"] % CONFIG)
        if "tracker" == here:
            print "[tracker] I want your states" 
            print "[tracker] IN %r" % (CONFIG["cnx"]["PUSH_any_tracker"] % CONFIG)
            cnx = context.socket(zmq.PULL)
            cnx.bind(CONFIG["cnx"]["PUSH_any_tracker"] % CONFIG)
            cnx_list["tracker_in"] = cnx

            cnx = context.socket(zmq.PUB)
            cnx.connect(CONFIG["cnx"]["PUB_tracker_any"]%CONFIG)
            
        else:
            cnx = context.socket(zmq.PUSH)
            print "[%s] =>tracker I will send you my states"  % here
            cnx.connect(CONFIG["cnx"]["PUSH_any_tracker"] % CONFIG)
            if here == "master":
                cnx_list["tracker_out"] = cnx
            else:
                cnx_list["tracker_out"] = cnx
            cnx = cnx_list["tracker_in"] = context.socket(zmq.SUB)
            cnx.connect(CONFIG["cnx"]["PUB_tracker_any"]%CONFIG)
            # tracker is the process dedicated to get the statuses.  
            print "[%s] tracker in is %s // %r " % (here, CONFIG["cnx"]["PUSH_any_tracker"] % CONFIG, cnx)
        if not len(cnx_list.get("next",{}).keys()):
            del(cnx_list["next"])
        print json.dumps( { k : repr(v) for k, v in cnx_list.items() }, indent = 4)
        return dict(cnx_list)

#compatibility with dsat 0.5.itworksforme
get_connection = Connector.get_connection
    # will be set to def() pass one day, this is my creative way of setting debug levels

#compatibility with dsat 0.5.itworksforme
def construct_info(ignored, func_or_name):
    return Connector.construct_info_from_cli(func_or_name)[1:]

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
        

    def _circus_process_alive(self,proc):
        return self._circus_cmd("numprocesses", proc)
        


    def circus_incr(self, process):
        """
        not to be used directly in fact
        """
        #logging.debug("incr %r" % process)
        try:

            self._circus_cmd("incr", process)
        except Exception as e:
            self._alive[process] -= 1
                        #D("ARG")
            logging.exception(e)


    def circus_decr(self, process): 
        """
        not to be used directly in fact
        """
        #logging.debug("incr %r" % process)
        try:
            self._circus_cmd("decr", process)
        except Exception as e:
            self._alive[process] += 1
            
            logging.exception(e)

    def _circus_cmd(self, cmd, process):
        #logging.debug("for <%s> calling <%s>  " % (process, cmd))
        return ProcTracker.circus.call( dict(
            command= cmd,
            properties = dict(
                name = process
            )
        )).get(cmd)

    def watch(self, busy_per_stage):
        """
        Process handling here.

        The more I look at it, the more I think it could be a function
        """
        for process, working in busy_per_stage.items():
            if process in self._alive:
                nb_running = self._circus_process_alive(process)
                low_precaution_margin =  self._config.get("minimal_worker_limit",1)
                high_precaution_margin =  self._config.get("minimal_worker_limit",3)
                if nb_running - working < low_precaution_margin:
                    self.circus_incr(process)
                    D("%s incr used(%d) available (%d) in [%d, %d]" % (
                    process, working , nb_running, low_precaution_margin,
                    high_precaution_margin))

                if nb_running - working > high_precaution_margin:
                    D("%s NOT DECREASING used(%d) available (%d) in [%d, %d]" % (
                    process, working , nb_running, low_precaution_margin,
                    high_precaution_margin))

            #    self.circus_decr(process)





