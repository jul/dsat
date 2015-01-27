import json 
from dsat.data import create_rrd_if , open_rrd, write_rrd
from dsat.state import state_wrapper
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

def rrd(cnx, ev, serialization = "simplejson"):
    if last_seen.get(ev["_type"], ev["_when"] ) != ev["_when"]:
        write_rrd(rrd_l[ev["_type"]], cfg, ev["_type"], ev)
    last_seen[ev["_type"]] = ev["_when"]

state_wrapper(sys.argv, rrd)
