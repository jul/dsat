import json 
from dsat.data import create_rrd_if , open_rrd, write_rrd
from dsat.state import Connector
from time import time
import sys
cfg = json.load(open("rrd.json"))
rrd_l = dict()

for rrd_db in cfg.keys():
    if rrd_db.startswith("_"):
       continue
    else:
        this_cfg = cfg[rrd_db]
        if isinstance(this_cfg, dict) and cfg[rrd_db].get("source"):
            try:
                create_rrd_if(cfg, rrd_db)
            except IOError:
                pass
            finally:
                rrd_l[rrd_db] = open_rrd(cfg, rrd_db)


last_seen=dict()

def rrd(cnx, payload, msg):
    now = time()
    if not msg["type"] in last_seen:
        last_seen[msg["type"]] = now
    if now - last_seen[msg["type"]] >= 1:
        write_rrd(rrd_l[msg["type"]], cfg, msg["type"], payload)
        last_seen[msg["type"]] = now

Connector(rrd).turbine()
