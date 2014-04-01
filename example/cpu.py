
import sys
from os import path, getpid
from dsat.state import state_wrapper
from dsat.message import send_vector
from dsat.linux_mtime import m_time as time
from random import randint
from time import sleep
from dsat.state import get_connection, construct_info
import sched

from time import sleep
import logging


cpu_f = open("/proc/loadavg")
def every(x):
    y =randint(0,x-1) 
    while True: 
        yield y == 0
        y = y - 1 if y>=0 else x-1

def cpu(cnx, arg):
    cpu_f.seek(0)
    _5,_10,_15 = cpu_f.read().split(" ")[:3]
    return { "data" : [ _5,_10,_15], "load" : _5, "5min" : _5, "10min" : _10, "15min" :  _15 }

cnt5 = every(5)
cnt3 = every(3)

state_wrapper(sys.argv, cpu, bounce_to=["proc", "ping"],
    cond_for=dict(
        proc=lambda ev: cnt5.next(),
        ping = lambda ev: cnt3.next(),
    )
)



