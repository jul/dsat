
import sys
from os import path, getpid
from dsat.state import Connector
import psutil
from time import sleep



def ping(cnx, arg, msg):
    measure = dict(psutil.network_io_counters().__dict__)
    measure["data"] = measure.values()
    return measure

Connector(ping).turbine()
