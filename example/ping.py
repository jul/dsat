
import sys
from os import path, getpid
from dsat.state import state_wrapper
import psutil

def ping(cnx, arg):
    measure = psutil.network_io_counters().__dict__
    measure["data"] = measure.values()
    return measure

state_wrapper(sys.argv, ping)

