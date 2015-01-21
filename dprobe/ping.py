
import sys
from os import path, getpid
from dsat.state import state_wrapper, construct_info
import psutil
from time import sleep

CFG, L_CFG, ID = construct_info(sys.argv, "ping")


def ping(cnx, arg):
    measure = dict(psutil.network_io_counters().__dict__)
    measure["data"] = measure.values()
    return measure

state_wrapper(sys.argv, ping)

