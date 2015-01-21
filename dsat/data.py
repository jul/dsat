# -*- coding: utf-8 -*-

from os import path
from pyrrd.rrd import DataSource, RRA, RRD
from time import time


def create_rrd_if(cfg, _type, **kw):
    cfg.update(kw)
    _path = cfg["_path"]
    if not path.exists(_path):
        raise IOError("Non existing path %(path)s" % cfg)
    fn = path.join(_path, _type.endswith(".rrd") and _type or "%s.rrd" % _type)
    if path.isfile(fn):
        #should check  RRD magic number if I was paranoid
        raise IOError("already exists %s" % fn)
    ds = [ DataSource(**{ 
            k:s_dct.get(k,cfg["_default"].get(k, "arg"))
                for k in set(s_dct.keys()) | set(cfg["_default"].keys())
        }) for s_dct in cfg[_type]["source"]]
    rra = [
    "RRA:MIN:0.5:10:57600",
    "RRA:AVERAGE:0.5:1:57600",
    "RRA:AVERAGE:0.5:10:57600",
    "RRA:AVERAGE:0.5:100:57600",
    "RRA:LAST:0:10:57600",
    "RRA:MAX:0:10:57600",
    ]
    archive = RRD(fn,rra=rra, ds=ds, **cfg.get("_rrd_option",{}))
    return archive.create()

def open_rrd(cfg,_type):
    _path = cfg["_path"]
    if not path.exists(_path):
        raise IOError("Non existing path %(_path)s" % _cfg)
    fn = path.join(_path, _type.endswith(".rrd") and _type or "%s.rrd" % _type)
    if not path.isfile(fn):
        #should check  RRD magic number if I was paranoid
        raise IOError("Does not exists %s" % fn)
    return RRD(fn)

def write_rrd(rrd_handler,cfg, _type, res_dict, time="N"):
    var = [ si["dsName"] for si in cfg[_type]["source"]]
    rrd_handler.bufferValue(":".join( [ str(time), ] + [ str(res_dict[k]) for k in var ] ))
    rrd_handler.update()

if "__main__" == __name__:
    import json 
    from dsat.data import create_rrd_if as cif
    cfg = json.load(open("rrd.json"))
    try:
        cif(cfg,"cpu")
    except:
        pass

    rrd = open_rrd(cfg, "cpu")
    write_rrd(rrd, cfg, "cpu", { "5min" :0, "10min": 0.1, "15min" : 0.2 })

