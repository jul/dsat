
import sys
from dsat.state import state_wrapper
from dsat.linux_mtime import m_time as time
from random import randint
from time import sleep
from dsat.state import construct_info

from time import sleep


cpu_f = open("/proc/loadavg")
def every(x):
    y =x 
    while True: 
        yield not y
        y = y - 1 if y>=0 else x-1



def cpu(cnx, arg):
    cpu_f.seek(0)
    _5,_10,_15 = cpu_f.read().split(" ")[:3]
    return { "data" : [ _5,_10,_15], "load" : _5, "5min" : _5, "10min" : _10, "15min" :  _15 }

cntproc = every(2800)
cntping = every(2800)
cntcsv = every(2800)
cntrrd = every(2800)

def cpu_clock(ev):
    #try:
   #sleep(float(ev['arg']['load']) * .0000)
    #except:
    #    pass
    return True

state_wrapper(sys.argv, cpu, bounce_to=["cpu", "proc", "ping"],
    cond_for=dict(
        proc=lambda ev: cntproc.next(),
        csvw = lambda ev: cntcsv.next(),
        rrd = lambda ev: cntrrd.next(),
        ping = lambda ev: cntping.next(),
        cpu = cpu_clock,
    )
)



