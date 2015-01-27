
import sys

from dsat.message import send_vector, incr_task_id
from dsat.linux_mtime import m_time as time
from random import randint
from time import sleep
from dsat.state import construct_info, Connector

from time import sleep

import os
import sched
from threading import Timer







ticker = sched.scheduler(time, sleep)


CFG, L_CFG, ID = construct_info(sys.argv, "cpu_clock")
cnx = get_connection(CFG, L_CFG)
#task_id = int(time())

vector = {
    "seq":0, "type" : "cpu","when" : 0, "event" : "INIT", 
    "next":"cpu", "job_id": "0",
    "task_id":0,"seq":0,
    "emitter" : "me(%d)" % os.getpid(),
    "serialization": "simplejson",
    "arg" : '{"load" : 0, "5min" : 0}', "where" : "localhost", 
    "step" :"master", "wid":"0", "pid":ctx.local_info["pid"] ,"retry":2 }


DELAY=0.001

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


while True:
    reschedule(ticker, vector, ctx.cnx["orchester_out"]



