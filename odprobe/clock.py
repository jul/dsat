
import sys

from dsat.message import send_vector, incr_task_id
from dsat.linux_mtime import m_time as time
from random import randint
from time import sleep
from dsat.state import construct_info, get_connection

from time import sleep
import logging
from logging.config import dictConfig
import os
import sched
from threading import Timer





smoothing_factor = 0

ticker = sched.scheduler(time, sleep)


CFG, L_CFG, ID = construct_info(sys.argv, "clock")
cnx = get_connection(CFG, L_CFG)
dictConfig(CFG.get("logging",{}))

#task_id = int(time())
log = logging.getLogger("orchester")
D = log.debug
vector = {
    "seq":0, "type" : "cpu", "event" : "INIT", 
    "next":"cpu", "job_id": "0",
    "task_id":"0","seq":0,
    "emitter" : "clock(%d)" % int(ID),
    "serialization": "simplejson",
    "arg" : '{"load" : 0, "5min" : 0}', "where" : "localhost", 
    "step" :"master", "wid":"0", "pid":L_CFG["pid"] ,"retry":2 }


DELAY=.01
smoothing_factor = None
def reschedule(scheduler, vector, socket):
    """push a job on socket at rescheduled interval
    rescheduler si task_id qui est dans what appartient aux taches active

    """
    global smoothing_factor
    try:
        next_delay = DELAY

        next_delay -= ( float(time()) % DELAY)
        #log.info("next event in %r" % next_delay)
        job_id = vector["type"],
        incr_task_id(vector)
        vector["seq"] = "0"
        send_vector(socket,vector)
        Timer(next_delay, reschedule,
            (ticker, vector, socket )
        ).start()
        D('job %r rescheduled' % job_id)
    except Exception as e:
        log.error("ARGGGGGG %r" % e)
        log.exception(e)
        sleep(1)

reschedule(ticker, vector, cnx["orchester_out"])

while True:
    global smoothing_factor
    ### use messages to get this info. and change clock frequency to avoid
    ### classical transition problems. 
    
    sleep(1)
    smoothing_factor = 0


