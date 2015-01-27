#!/usr/bin/env python
# -*- coding : "utf-8" -*-
import sys
import os
from dsat.state import Connector
#get_connection, construct_info
import sched
from dsat.message import send_vector
from dsat.linux_mtime import m_time as time
from time import time
from threading import Timer
from random import randint







#ticker = sched.scheduler(time, sleep)


#CFG, L_CFG, ID = construct_info(sys.argv, "master")
#cnx = get_connection(CFG, L_CFG)
#task_id = int(time())



def master(ctx, payload, msg):
    ctx.log.debug("I received %r" % msg)

ctx = Connector(master)

ev = {
    "seq":0, "type" : "cpu","when" : 0, "event" : "INIT", 
    "next":"cpu", "job_id": "0",
    "task_id":0,"seq":0,
    "emitter" : "me(%d)" % os.getpid(),
    "serialization": "simplejson",
    "arg" : '{"load" : 0, "5min" : 0}', "where" : "localhost", 
    "step" :"master", "wid":"0", "pid":ctx.local_info["pid"] ,"retry":2 }

send_vector( ctx.cnx["orchester_out"], ev)

ctx.turbine()    

    