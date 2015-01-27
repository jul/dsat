#!/usr/bin/env bash

rrdtool graph this.png  --height 600 --start now-8h  --width 800 "DEF:10min=data/cpu.rrd:10min:AVERAGE" "DEF:5min=data/cpu.rrd:5min:AVERAGE"  "DEF:15min=data/cpu.rrd:15min:AVERAGE"  LINE1:5min#0000FF:"5" LINE1:10min#00FF00:"10 min" LINE1:15min#FF0000:"15 min" 
display -delay 6 this.png & 
while [[  1 ]]; do 
    sleep 5.01
    rrdtool graph this.png  --height 600 --start now-2h  --width 800 "DEF:10min=data/cpu.rrd:10min:AVERAGE" "DEF:5min=data/cpu.rrd:5min:AVERAGE"  "DEF:15min=data/cpu.rrd:15min:AVERAGE"  LINE1:5min#0000FF:"5" LINE1:10min#00FF00:"10 min" LINE1:15min#FF0000:"15 min" 
done

