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
        incr_seq, decr_seq, re_send_vector
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
CONFIG, LOCAL_INFO, ID = construct_info(sys.argv, "tracker")

dictConfig(CONFIG.get("logging",{}))
log = logging.getLogger()
CONFIG.update(LOCAL_INFO)
D = log.info

def q_madd(q, *what):
    if what[-1] == what[-2]:
        D("WUUUUUTTT %r" % what)
        raise Exception("ARRRRGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG")
    q.put("\0".join(what))

def q_mget(q):
    return q.get().split("\0")



D("Started tracker")

CNX = get_connection(CONFIG, LOCAL_INFO)
from pprint import PrettyPrinter as PP
pp = PP(indent=4)
P = pp.pprint
P(dict(CNX))

timer_q = Queue()


def watcher(config):
    proc_tracker = ProcTracker(config)
    delay = config.get("check_delay",10)
    sleep(delay)
    while True:
        for proc in proc_tracker._alive:
            proc_tracker.watch(proc)
        sleep(delay)

ProcWather = Process(target=watcher, args=(CONFIG,))
ProcWather.start()

def time_keeper(timer_q, config):
    task_manager = TimerProxy()
    monitor_me = CNX["tracker_in"]
    conf = config["state_keeper"]
    conf["readonly"] = True
    state_reader = State(conf)
    def id_maker(j_id, t_id):
        return ":".join([str(j_id), str(t_id)])

    while True:
        try:
            cancel_or_start, job_id, task_id = q_mget(timer_q)
            my_id = id_maker(job_id, task_id)
            sleep(.5)
            if CONFIG.get("debug_integrity"):
                D("Scheduling/Canceling %s =>  %s(%s,%s)" % (
                    cancel_or_start,  my_id, task_id, job_id))
            state = None
            res = task_manager.cancel_event(my_id)
            if "cancel" == cancel_or_start:
                return
            retry = config.get("try_reading", 2)
            while not state and retry:
                try:
                    state = state_reader.get(job_id, task_id)
                except KeyError:
                    ### RACE CONDITION between reader and writer
                    state = False
                    sleep(config.get("sleep_after_reading_state", .1))
                finally:
                    retry -= 1
            if not state:
                raise KeyError("<%r> not present in states" % my_id)


            state["event"] = "TIMEOUT"
            state["retry"] = str(int(state.get("retry", "0")) + 1 )
            state = state.copy()
            task_manager.add_event_in(
                config["timeout"] , my_id,
                send_vector, monitor_me,{k:v for k,v in  state.items() if
                    not k.startswith("_")} 
            )
            #if len(task_manager._active_task):
            #    D("<%d> timeout watcher in queue" % len(task_manager._active_task))
            #sleep(.01)
        except Exception as e:
            #log.exception("Failed in timekeeper %r" % e)
            #log.exception("Failed in timekeeper %r" % e)
            log.warning(e)

TimeWather = Process(target=time_keeper, args=( timer_q, CONFIG))
TimeWather.start()


def event_listener(CNX, timer_q,  config):
    """Processlet responsible for routing and reacting on status change"""
    D("event listener")
    cnx = CNX

    state_keeper = State(config["state_keeper"])


    def state_changer(state_name):
        def wrapper(func):
            def wrapped(state_keeper, old, new):
                res = SENTINEL
                if "INIT" == new["event"]:
                    # TODO RESTABLISH thjat
                    #re_send_vector(cnx["tracker_out"], new)
                    pass
                #D("SEQ??? %(step)s %(event)s %(seq)s" % new)
                else:
                    old = state_keeper.get(new["job_id"], new["task_id"])

                try:
                    if config.get("debug_transition", False) or \
                            new["event"] in { "INIT","HAPPY_END" }:
                        D("ENTER Transition [%s] applied to state[%s-seq[%s]]"
                            "for <%s>" % (
                                new["event"],
                                old["state"],
                                old["seq"],
                                "%(job_id)s-%(task_id)s %(seq)s" % new
                            )
                        )
                    new["state"] = state_name
                    if not isinstance(new, state_keeper.Record):
                        new = state_keeper.Record(new)
                    res =  func(state_keeper, old, new)
                    if config.get("debug_transition", False):
                        D("RES VECT %s" % _f(new))
                except Exception as e:
                    msg = "KO TRANS %s -> %s for <%s>reason %r " % (
                        new.get("state", "????"), state_name,
                        "%(job_id)s-%(task_id)s" % new, e
                    )
                    logging.exception(msg + "reason <%r> : data : <%r>" % (e, new))
                    #logging.critical(msg + "reason <%r> : data : <%r>" % (e, new))
                finally:
                    if "HAPPY_END" not in [ new["state"], old["state"]]:
                        state_keeper.update(new)
                    
                    # TODO RESTABLISH thjat
                    re_send_vector(CNX["tracker_out"], new)

                if res is not SENTINEL:
                    return res
            return wrapped
        return wrapper

    
    @state_changer("FAILED")
    def on_failure(state_keeper, old, new):
        raise Exception("KO for <%(job_id)r-%(task_id)r> %(arg)r" % new)

    @state_changer("PENDING")
    def on_ack(state_keeper, old, new):
        new["retry"]="0"
        q_madd(timer_q,"cancel", new["job_id"], new["task_id"])

        #logging.info("OK <%(job_id)r-%(task_id)s> " % (new) + "at step %(step)s" % old )

    @state_changer("JOB_PROCESSED")
    def on_job_processed(state_keeper, old, new):
        #D("JOB PROCESSED @ STEP %(step)s" % new)
        new["retry"]="0"
        #D("increasing ???")

    @state_changer("HAPPY_END")
    def happy_end(state_keeper, old, new):
        cleanUp(state_keeper, old, new)

    def clean_out_of_order(event):
        for i in range(0, int(event["seq"]) +1 ):
            try:
                del(unordered_msg[
                    (event["job_id"], event["task_id"], str(i))
                ])
            except KeyError:
                pass

    def cleanUp(state_keeper, old, new):
        #D("cleaning resource for <%(job_id)r-%(task_id)s> %(arg)r" % new)
        q_madd(timer_q,"cancel", new["job_id"], new["task_id"])
        #proc_tracker.incr(new["step"])

        try:
            state_keeper.delete(new["job_id"], new["task_id"])
        except Exception as e:
            if CONFIG.get("debug_integrity"):
                logging.exception(
                    "Integrity loss in status for %r reason :%r" % (new, e)
                )



    @state_changer("PROCESSING")
    def on_start(state_changer, old, new):
       # watchdog_q.put(("watch", new["step"]))
       pass



    @state_changer("TIMEOUT")
    def on_timeout(state_keeper,old, new):
        __on_error(state_keeper, old, new)

    @state_changer("ERROR")
    def on_error(state_keeper,old, new):
        __on_error(state_keeper, old, new)


    def __on_error(state_keeper, old, new):
        #watchdog_q.put(("incr", new["step"]))
        new["retry"] = str(int(old["retry"] ) + 1 )


        D("OE %s->%s" % (_f(old), _f(new) ))
        #### WHY did I put that? 
        state_keeper.update(new)
        if  "FAILED" == old.get("state","FAILED"):
            D("What is it?")
        if int(new["retry"]) >= CONFIG["max_retry"]:
            D('retry %(retry)s for <%(job_id)s-%(task_id)s>' % new)
            new["event"] = "FAILURE"
            state_keeper.update(new)
            on_failure(state_keeper, old, new)
        else:
            log.critical("unhandled failure for %r" % new)
            ### could also restart failing processes here




    @state_changer("PUSHING")
    def on_send(state_keeper, old, new):
        # incr_vector?

        q_madd(timer_q,"start", new["job_id"], new["task_id"])



    @state_changer("INITING")
    def on_init(state_keeper, old, new):
        new["event"] = "SEND"
        D("<%r:%r> IN  <%r>" % (new["type"], cnx[new["type"]], cnx))
        #send_vector([ cnx[new["type"]]], new)


    def on_propagate(state_keeper, old, new):
        D("just passing %s" % _f(new))
        new["event"] = "PROPAGATE"
        #re_send_vector(cnx[new["type"]], new)

    D("waiting")
    transitions = dict(
            INIT = on_init,
            SEND = on_send,
            HAPPY_END = happy_end,
            ACK = on_ack,
            END_OF_JOB = on_job_processed,
            START = on_start,
            PROPAGATE = on_propagate,
            ERROR = on_error,
            TIMEOUT = on_timeout,
            O_TIMEOUT = on_failure,
    )
    
    unordered_msg = expiringCache(
            CONFIG.get("max_unordered_event", 10000),
            CONFIG.get("expiring_seq_sec", 3600))

    def get_lowest_seq(task):
        i_seq = int(task["seq"])
        for i in range(i_seq):
            lower =  unordered_msg.get(tuple([
                        task["job_id"], task["task_id"], str(i)
                    ])
                    )
            if lower:
                return lower
        
    def next_in_line(old, new):
        if CONFIG.get("debug_next_in_line"):
            D("diff %r -%r" % (_f(new), _f(old)))
        #D("Pending messages <%s> " % "|".join(map(repr,unordered_msg.keys())))
        #D("diff seq is %d" % ( int(new["seq"])  - int(old["seq"])))
        #if old["seq"] == new["seq"] and new["event"] != old["event"]:
        #    D("WHY?!!")
        #    return new
        #dyslexia, what is > or > ?
        lowest_ooo = new if old is new  else get_lowest_seq(new) 
        ## lowest_ooo_seq => ooOrder
        if lowest_ooo:
            #D("LOWEST %s" % _f(lowest_ooo))
            #D("NEWEST %s" % _f(new))
            unordered_msg.put(
                tuple([ new["job_id"], new["task_id"], new["seq"]]),
                new
            )
            if 1 == int(new["seq"]) - int(lowest_ooo["seq"] ):
                return lowest_ooo, new
            
            next = unordered_msg.get(
                tuple([ lowest_ooo["job_id"], lowest_ooo["task_id"],
                    str(int(lowest_ooo["seq"])+1)
                ])
            )
            if next:
                if CONFIG.get("debug_next_in_line"):
                    D("successor in stack")
                return lowest_ooo, next


        if 1 ==  int( new["seq"] ) - int(old["seq"]):
            if CONFIG.get("debug_next_in_line"):
                D("OREDER %r" % _f(new))
            return old, new
        
        unordered_msg.put(
            tuple([ new["job_id"], new["task_id"], new["seq"]]),
            new
        )
        unordered_msg.put(
            tuple([ new["job_id"], new["task_id"], old["seq"]]),
            old
        )
        return False, False

    
    def play_transition_in_order(state_keeper, new):
        task_id = new["task_id"]
        job_id = new["job_id"]
        abort_me = False
        try:
            old = state_keeper.get(job_id, task_id)
        except KeyError:
            if "HAPPY_END" == new["state"]:
                cleanUp(state_keeper, new, new)
            abort_me = True
        if abort_me:
            return

        ### values stored in satlive and not in messages needs to be
        ### regeneratied
        #new.update( { k :v for k,v in old.items() if k not in new} )
        if "HAPPY_END" == old["state"]:
            next = False
        else:
            old, next = next_in_line(old, new)

        while next:
            if CONFIG.get("debug_pending_messages"):
                D("COMPUTING %s => %s" % (old["seq"], next["seq"]))
            transitions[next["event"]](state_keeper, old, next)
            unordered_msg.invalidate(
                tuple([old["job_id"], old["task_id"], old["seq"]])
            )
            if CONFIG.get("debug_pending_messages"):
                D("Pending messages <%r> " % unordered_msg)
                pass
            if "HAPPY_END" == next["state"]:
                next = False
            else:
                state_keeper.update( next )
                old, next = next_in_line(next, next)
            

    print("Waiting for socket to be read cf 100% CPU zmq bug")
    sleep(1)
    local_in_sox = cnx["tracker_in"]
    print LOCAL_INFO

    while True:
        new = parse_event(local_in_sox)
        D("RCV %s" % _f(new))
        if new["where"] != LOCAL_INFO["where"]:
           D("NOT FOR ME %s" % _f(new))
           continue
        try:
        # only one message at a time can be treated not even sure I need it
            task_id = new["task_id"]
            job_id = new["job_id"]
            if "INIT" == new["event"]:
                new["state"] = "INIT"
                new["retry"] = "0"
                new["step"] ="satlive"
                new["next"] =  new["type"]
                #send_vector(cnx[new["type"]], new)
                #if not(state_keeper.get(new["job_id"],new["task_id"])):
                state_keeper.add( new )
                transitions["INIT"](state_keeper, new, new)
                continue
            if "HAPPY_END" == new["event"]:
                happy_end(state_keeper, new,new)


            if int(new["seq"]) > config.get("max_seq", 50):
                logging.warning("<%r> was bounced <%r> times" %(new,
                    new["seq"]))
                transitions["HAPPY_END"](state_keeper, new, new)
                continue
            try:
                old = state_keeper.get(new['job_id'], new['task_id'])
            except KeyError:
                D("OUT OF ORDER ORPHANED MESSAGE %s" % _f(new))

                retry = 2
                ok = False
                while not ok and retry:
                    sleep(CONFIG.get("adjust_state_reader_latency", .1))
                    try:
                        old = state_keeper.get(new['job_id'], new['task_id'])
                        ok = True
                    except KeyError:
                        pass
                    finally:
                        retry -= 1
                if not ok: 
                    if "HAPPY_END" != new["event"]:
                        logging.warning("Garbage state : %r" % _f(new)) 
                        continue
                    else:
                        cleanUp(state_keeper, new, new)


            if "PROPAGATE" == new["event"]:
                on_propagate(state_keeper, new, new)
                D("skipping PROPAGATE for %s" % _f(new))
                continue

            play_transition_in_order(state_keeper, new)
        except Exception as e:
            log.exception("MON %s" % e)





### rule of thumbs every queue should be used twice and only twice

event_listener(CNX, timer_q, CONFIG )


