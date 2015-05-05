#!/usr/bin/env python
# -*- coding: utf8 -*-
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from contextlib import closing
from sys import stderr
import atexit

"""Utility to probe if a satellite tcp connexion is alive
Hint: zmq does not tell you if the connection is alive"""

# padding in ** 2
SO_BEACON = 8 
MSG_BEACON = "alive"

def semaphore(cfg):
    server_address = ( str(cfg["host"]), int(cfg["port"]))
    while True:
        with closing(socket(AF_INET, SOCK_STREAM)) as tcp_sock:
            atexit.register(tcp_sock.close)
            tcp_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            try:
                tcp_sock.bind(server_address)
                tcp_sock.listen(1)
                while True:
                    connection, client_address = tcp_sock.accept()
                    connection.sendall(MSG_BEACON)
                    connection.close()
            except Exception as e:
                print >>stderr, "Exception %r occurred in semaphore" % e
                exit (4)

def lookout(cfg):
    acceptable_latency = float(cfg["timeout"])
    server_address = ( str(cfg["host"]), int(cfg["port"]))
    with closing(socket(AF_INET, SOCK_STREAM)) as tcp_sock:
        try:
            tcp_sock.settimeout(acceptable_latency)
            tcp_sock.connect(server_address)
            data = tcp_sock.recv(SO_BEACON)
            if data == MSG_BEACON:
                return True
        except Exception as e:
            #print >>stderr, "Missed a beacon in the veil of the night %r" % e
            #print "."
            pass
    return False


    

