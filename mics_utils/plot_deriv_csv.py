#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import pylab as plt
import numpy as np
import datetime as dt
from time import sleep
import matplotlib.dates as md
import sys
plt.ion()
while True:
#if True:
    
    data = np.genfromtxt(sys.argv[1], delimiter =",", 
            names = [ "time", "x", "y",  ])
    date = md.date2num(map(dt.datetime.fromtimestamp, data["time"]))
    xfmt = md.DateFormatter("%Y-%m-%d %H:%M:%S")
    plt.xticks( rotation=25)
    plt.subplots_adjust(bottom= 0.2)
    re_zero = lambda d: [ i and i or 1 for i in d ]

    ax1 = plt.gca()
    DTS=min(len(date),  100)
    ax1.xaxis.set_major_formatter(xfmt)
    ax1.plot(date[-DTS+1:],
            np.diff(data["x"][-DTS:])/re_zero(np.diff(data["time"][-DTS:])) , 'r.', label ="x" )
    ax1.plot(date[-DTS+1:], 
            np.diff(data["y"][-DTS:])/re_zero(np.diff(data["time"][-DTS:])) , 'g.',
            label ="y" )
    leg = ax1.legend
    plt.draw()
    #plt.show()
    sleep(20)


