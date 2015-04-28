#!/usr/bin/env python
# -*- coding: utf-8 -*-

from time import time, sleep, asctime as _asctime
import sched
from random import randint
import logging
import logging.handlers
import sys,os
import readline
from readline import write_history_file, read_history_file
import zmq
from simplejson import dumps, load, loads
from dsat.message import send_vector, fast_parse_vector, extract_vector_from_dict
from dsat.state import _f
import dsat

print dsat.__version__

_to = sys.argv[1]
_mode = sys.argv[2]
_stable = sys.argv[3] == "bind"
_what = None if len(sys.argv) <= 4 else sys.argv[4]

my_logger = logging.getLogger('Logger')
my_logger.setLevel(logging.DEBUG)
handler = logging.handlers.SysLogHandler(address = '/dev/log')
my_logger.addHandler(handler)
def D(msg):
    my_logger.warning("%r:%s" % (os.getpid(), msg))

HIST = ".hist_zmq"
if not os.path.exists(HIST):
    write_history_file(HIST)
read_history_file(HIST)


### init phase load its parameters 


context = zmq.Context()
client = context.socket(getattr( zmq, _mode))
sleep(1)
_boc =  _stable  and "bind" or "connect"
_cnx_mode = getattr(client, _boc )
_cnx_mode(_to)
if _mode == "SUB":
    client.setsockopt(zmq.SUBSCRIBE, '')
    print "USBSRCIRINB ALL"
sleep(1)


print "address: %r" % _to
print "PATTERN: %r" % _mode
print _boc

print "message template is: %s" % dumps(extract_vector_from_dict({}), indent=4)

abort = False
recv = False
message=_what
while message and not abort:
    if "q" == message:
        break
    if "r" == _what:
        recv=True
    elif _what:
        message = _what
        abort = True
    else:
        message = "".join(iter(lambda :raw_input("%s >" % _to), "ç"))
    try:
        if recv:
            cpt = 0
            while True:
                print "waiting ..."
                print  fast_parse_vector(client)
                print "RECEVIED"
                print (" " * cpt ) + [ "\\", "-" , "/" , "|" ][cpt%4]
                cpt += 1
        else:
            print("SENT %s" % loads(message))
            print "\n"
            print client.socket_type
            send_vector(client, loads(message))

    except Exception as e:
        print(repr(e))

    D("sent %r" % message)
write_history_file(HIST)
