import sys
from os import path, getpid
from dsat.state import state_wrapper, construct_info
from dsat.carbon import carbon_maker
from archery.bow import Hankyu as dict
import psutil
from time import time, sleep

CFG, L_CFG, ID = construct_info(sys.argv, "proc")

send = carbon_maker(**CFG)


def proc(cnx, arg):
    res= dict()

    sum_file = dict()
    procc= dict()
    sum_connection = dict() 
    percent_mem= dict()
    all_proc = psutil.get_process_list()
    carbon_measure = dict()
    interesting = { 
        '/usr/lib/firefox/firefox', '/opt/google/chrome/chrome', 'mysqld', 
        'mongod', "ping.py", 
        "proc.py", "master.py", "tracker.py","cpu.py", "rrd.py", "csvw.py"}
    for x in all_proc:
        try:
            key = ( set(x.cmdline()) & interesting) and "me" or "other"
            carbon_key=None
            cmd = x.cmdline()
            intersect =  interesting &set(cmd)
            if intersect:
                assert len(intersect) == 1
                carbon_key = intersect.pop()
                
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
                if carbon_key:
                    carbon_measure += { carbon_key: x.get_memory_percent() }
            except:
                pass
        except:
            pass
    send(carbon_measure)
    ratio = lambda d : min(1,1.0 *d.get("me", 0)/max(.0001,d.get("other",0)))
    absol = lambda d : d.get("me", 0) + d.get("other", 0)
    res=dict(sum_file= sum_file, percent_mem= percent_mem, all_proc =len(all_proc))
    res["data"] = map(ratio, [ sum_file, sum_connection, percent_mem, procc])
    res["data"] += map(absol, [sum_file, sum_connection, percent_mem, procc])
    del all_proc
    return res


state_wrapper(sys.argv, proc)

