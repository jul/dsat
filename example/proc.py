import sys
from os import path, getpid
from dsat.state import state_wrapper
from archery.bow import Hankyu as dict
import psutil

def proc(cnx, arg):
    res= dict()
    sum_file = dict()
    procc=dict()
    sum_connection = dict() 
    percent_mem= dict()
    all_proc = psutil.get_process_list()
    interesting = { "ping.py", 
        "proc.py", "master.py", "tracker.py","cpu.py", "rrd.py", "csvw.py"}
    for x in all_proc:
        key = ( set(x.cmdline()) & interesting) and "me" or "other"
        try:
            procc += dict({key : 1 })
        except:
            pass


        try:
            sum_file += dict({key :x.get_num_fds()})
        except:
            pass

        try:
            sum_connection += dict({ key: sum(x.get_num_ctx_switches())})
        except:
            pass
        try:
            percent_mem += dict({key : x.get_memory_percent() })
        except:
            pass
    ratio = lambda d : min(1,1.0 *d.get("me", 0)/max(.0001,d.get("other",0)))
    absol = lambda d : d.get("me", 0) + d.get("other", 0)
    res=dict(sum_file= sum_file, percent_mem= percent_mem, all_proc =len(all_proc))
    res["data"] = map(ratio, [ sum_file, sum_connection, percent_mem, procc])
    res["data"] += map(absol, [sum_file, sum_connection, percent_mem, procc])
    return res


state_wrapper(sys.argv, proc)

