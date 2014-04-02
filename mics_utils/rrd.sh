!/usr/bin/env bash

rrdtool graph this.png  --height 600 --start end-5h --width 800 "DEF:10min=data/cpu.rrd:10min:AVERAGE" "DEF:5min=data/cpu.rrd:5min:AVERAGE"  "DEF:15min=data/cpu.rrd:15min:AVERAGE"  LINE1:5min#0000FF:"default resolution" LINE1:10min#00FF00:"10 min" LINE1:15min#FF0000:"15 min" && display this.png 

