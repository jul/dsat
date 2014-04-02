#!/usr/bin/env python
# -*- coding : "utf-8" -*-
import sys
from dsat.state import get_connection, construct_info
import sched
from dsat.message import send_vector
from dsat.linux_mtime import m_time as time
from time import sleep

CFG, L_CFG, ID = construct_info(sys.argv, "master")
cnx = get_connection(CFG, L_CFG)
DELAY=3
task_id = int(time())
ev = {
    "seq":0, "type" : "cpu","when" : time(), "event" : "INIT", 
    "next":"orchester", "job_id": "0",
    "task_id":task_id,"seq":0, 
    "arg" : {"5min" : 0}, "where" : "localhost", 
    "step" :"master", "wid":"0", "pid":L_CFG["pid"],"retry":0 }
sleep(DELAY )
send_vector(cnx["orchester"], ev)

from threading import Timer
from random import randint

ticker = sched.scheduler(time, sleep)

def reschedule(scheduler, vector, socket):
    """push a job on socket at rescheduled interval
    rescheduler si task_id qui est dans what appartient aux taches active
  
TOFIX  loses one job a day w 300 sec interval
TODO find the hidden monotonic time function
    """
    try:
        next_delay = when = int(task.get("arg",{}).get("_every", DELAY))

        next_delay -= ( int(m_time()) % when)
        job_id = task["type"],
        task["task_id"] = str(int(task["task_id"]) + 1)
        task["seq"] = "0"
        send_vector(socket,vector)
        Timer(next_delay, reschedule,
            (ticker, task, socket )
        ).start()
        D('job %r rescheduled' % job_id)
    except Exception as e:
        logging.exception("ARGGGGGG %r" % e)
