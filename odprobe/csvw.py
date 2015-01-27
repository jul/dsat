import sys
from dsat.state import Connector
### TIME IS BAD
from time import time
if not len(sys.argv) >= 1:
    raise( Exception("Arg"))

import csv
from os import path

last_seen=dict()

def csvw(cnx, payload, msg):
    now = time()
    if not msg["type"] in last_seen:
        last_seen[msg["type"]] = now
    if now - last_seen[msg["type"]] >= 1:
        with open(path.join("data", "%(type)s.csv" % msg ), "a") as f:
            c_write = csv.writer(f)
            ### time() == pas bien
            c_write.writerow([ str(int(time())) ] + payload["data"]  )

        last_seen[msg["type"]] = now
    


Connector(csvw).turbine()

