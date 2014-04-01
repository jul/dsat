import sys
from dsat.state import state_wrapper

if not len(sys.argv) >= 1:
    raise( Exception("Arg"))
import csv
from os import path
import logging

def csvw(cnx, ev):
    from json import dumps
    with open(path.join("data", "%(_type)s.csv" % ev ), "a") as f:
        c_write = csv.writer(f)
        c_write.writerow([ int(float(ev["_when"])) ] + ev["data"]  )



state_wrapper(sys.argv,csvw)

