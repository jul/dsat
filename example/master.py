
import sys
from dsat.state import get_connection, construct_info
from dsat.message import send_vector
from dsat.linux_mtime import m_time as time
from time import sleep

CFG, L_CFG, ID = construct_info(sys.argv, "master")
cnx = get_connection(CFG, L_CFG)
task_id = 0
DELAY = 5
MAXBIT = 32
while True:
    task_id = int(time()) + ( ( task_id + 1) & (( 1<<MAXBIT) - 1))
    ev = {
        "seq":0, "type" : "cpu","when" : 123, "event" : "INIT", 
        "next":"orchester", "job_id": "0",
        "task_id":task_id,"seq":0, 
        "arg" : {"5min" : 0}, "where" : "localhost", 
        "step" :"master", "wid":"0", "pid":L_CFG["pid"],"retry":0 }
    sleep(DELAY - ( time() % DELAY) )
    send_vector(cnx["orchester"], ev)
