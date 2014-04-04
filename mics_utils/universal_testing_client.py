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
from json import dumps, load, loads
from dsat.message import send_vector
from dsat.state import _f
from zmq.utils import jsonapi

_to = sys.argv[1]
_mode = sys.argv[2]
_stable = sys.argv[3] is "bind"
_send = sys.argv[4]
_what = None if len(sys.argv) <= 5 else sys.argv[5]

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
_cnx_mode = getattr(client, "bind" if _stable else "connect" )
_cnx_mode(_to)

print "to: %r" % _to
print "mode : %r" % _mode
print "bind or connect? %r " % getattr(client, "bind" if _stable else "connect")
print "send_mode %r " % getattr(client, _send)
message = ""

while _what != None or message != "q" :
    if not _what:
        message = raw_input("%s >" % _to)
        if "q" == message:
            break
    else:
        message=_what
        _what = None
    try:
        print("/// %r" % _f(loads(message)))
        send_vector(client, loads(message))

    except Exception as e:
        print(repr(e))

    D("sent %r" % message)
write_history_file(HIST)
