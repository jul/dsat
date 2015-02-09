
import sys

from dsat.message import send_vector
from dsat.carbon import carbon_maker
from dsat.linux_mtime import m_time as time
from random import randint
from time import sleep
from dsat.state import construct_info, Connector

from time import sleep


cpu_f = open("/proc/loadavg")
def internal_clock():
    clock=0
    while True:
        clock+=1
        yield clock

def next_tick():
    return internal_clock().next()

def every(x):
    y = x 
    while True: 
        yield not y
        y += 1
        y = y%x

frequency = dict(
    proc = every(100),
    ping = every(100),
)


def reroute_to(connector, frequency, vector):
    for dest, freq in frequency.items():
        if freq.next():
            new_vector = vector.copy()
            send_vector(connector.cnx["orchester_out"], new_vector, "INIT", 
                dict(
                    type = dest,
                    next = dest,
                    seq = 0,
                    job_id = dest,
                    task_id = next_tick()
                )
            )
    
def cpu_clock(connector,_5, vector):
    #try:
    #sleep(float(_5) * .1 )
    new_vector = vector
    try:
        send_vector(connector.cnx["orchester_out"], new_vector, "INIT", 
            dict(
                type = "master",
                next = "master",
                seq = 0,
                job_id = "clock",
                task_id = next_tick())
            )

        sleep( (float(_5) ** 2 ) * .01 )
    except Exception as e:
        connector.log.exception(e)
        raise Exception("AARG %r" % e)





connector = Connector("cpu")

carbon_send = carbon_maker(connector)
def cpu(connector, payload, vector):
    cpu_f.seek(0)
    _5,_10,_15 = cpu_f.read().split(" ")[:3]
  #  cpu_clock(connector,_5 , vector)
    reroute_to(connector, frequency, vector)
    if int(vector["task_id"]) % 100 == 0:
        carbon_send( { "5m" : float(_5), "10m" : float(_10), "15m" :  float(_15) })
    return { "data" : [ _5,_10,_15], "load" : _5, "5min" : _5, "10min" : _10, "15min" :  _15 }

connector.func = cpu

connector.turbine()



