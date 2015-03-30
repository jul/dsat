#!/usr/bin/env python
# -*- coding: utf-8 -*-

from archery.bow import Daikyu as sdict
import sys
import atexit



from logging.config import dictConfig
import logging

from itertools import islice, product
from dsat.state import construct_info, get_connection


#pyzmq is string agnostic, so we ensure we use bytes
import simplejson as json
from simplejson import loads, dumps


SENTINEL = object
#### let's 
#time_keeper = scheduler(time, sleep)
if not len(sys.argv) >= 1:
    raise( Exception("Arg"))


class ToxicSet(set):
    """a set for wich add is a shortcut for union

    Some people want to watch the world burn.
    """
    def copy(self):
        if hasattr(super(Copier, self),"copy"):
            return self.__class__(super(Copier, self).copy())
        else:
            return [ x for x in self]

    def __iadd__(x,y):
        x|=y
        return x

    def __add__(x,y):
        return x|y


    def to_json(self):
        return list(self)

    def __str__(self):
        return str(list(self))

    def __repr__(self):
        return repr(list(self))


#CNX = get_connection(CONFIG, LOCAL_INFO)


class Backend(object):
    """A class that does a garbage collector on a list of event per channel
    each channel must be in _db
    in _ref_counter => channel_name => lurker there is a a ref counters on the events
    that were consumed
    Since union/removal/intersections are used a lot, it is a set that is chosen
    for each channel there is a Cache like API to serialize/unserialize the events
    being stored. 

    Events are stored as channel => event_id => event

    the cache is invalidated if the ref count is 0 on a given event_id for all lurkers
    the cache is destroyed if nb lurkers = 0
    """
    # list of lurkers per region
    _db = None
    # list of reference counts per channel per lurkers
    ## requires that keys are unique for EVERY region (hence the use of stamper)
    _ref_counter = sdict()

    def _dump(self):
        ### nice to have also save the cache config ..... 
        return { ch : { cons : task_s.to_json() 
                for cons, task_s in chan_ref.items() 
                }
                for ch, chan_ref in Backend._ref_counter.items()
        }
    def _load(self, json_dict):
        ### nice to have also load the cache config ..... 
        for channel in self.db():
            self.drop(channel)
        Backend._ref_counter = sdict({ ch : sdict({ cons : ToxicSet(task_s) 
                for cons, task_s in chan_ref.items() 
                })
                for ch, chan_ref in json_dict.items()
        })
        for channel in Backend._ref_counter:
            self.db(channel)

    def save(self, name):
        with open(name, "w") as save_there:
            json.dump(self._dump(), save_there)

    def load(self, name):
        with open(name) as backup :
            self._load(json.load(backup))
            self.sync()

    def sync(self):
        task_per_chan = sdict()
        for chan, gc in Backend._ref_counter.items():
            task_per_chan += sdict({ chan :ToxicSet( [ l for x in  gc.values() for l in x]) })
        new_ref_counter = Backend._ref_counter.copy()
        print task_per_chan
        print new_ref_counter
        for chan, tasks in task_per_chan.items():
            for task_id in tasks:
                # dont test for False but for Non existent values
                # still payload that are {} are as interesting as nothing
                if not self.get(chan, task_id):
                    for cons in Backend._ref_counter[chan]:
                        new_ref_counter[chan][cons] -= ToxicSet({ task_id })
        del(Backend._ref_counter)
        Backend._ref_counter = new_ref_counter

    def __init__(self, **option):
        self.name = option.get("name", 'garbage_collector')
        if Backend._db is None:
            Backend._db = sdict()
        if "cache_maker" not in option:
            from repoze.lru import LRUCache
        self.cache_maker = option.get("cache_maker",
            lambda name: LRUCache(10000)
        )
        Backend._ref_counter = Backend._ref_counter

    def register(self, channel_name, lurker):
        """register a lurker for channel"""
        if channel_name not in Backend._db:
            self.db(channel_name)
            Backend._ref_counter[channel_name] = sdict()
        Backend._ref_counter[channel_name] += sdict( {lurker : ToxicSet([]) })

    def unregister(self, channel_name, lurker):
        if channel_name not in Backend._db:
            raise KeyError("Absent channel in DB %r" % channel_name)
        #### TODO potential memory leak
        #### dec_ref all task_id in the ref_counter 
        #### would avoid some memory leak
        for task in Backend._ref_counter[channel_name][lurker]:
            self.dec_ref(channel_name, lurker, task)

        del(Backend._ref_counter[channel_name][lurker])
        if not Backend._ref_counter[channel_name]:
            self.drop(channel_name)

    def db(self, name = SENTINEL):
        if name is SENTINEL:
            return Backend._db.keys()
        if name not in Backend._db:
            Backend._db[name] = self.cache_maker(name)
        return Backend._db[name]

    def set_ref(self, channel_name, event_id):
        """sdict ajoute les valeurs aux feuilles des arbres à moins que 
        la valeur soit un mapping ça évites d'écrire du code"""
        Backend._ref_counter[channel_name] += ToxicSet([ event_id ])

    def dec_ref(self, channel_name, consumer, event_id):
        seen=False
        Backend._ref_counter[channel_name] -= sdict({ 
                consumer :ToxicSet([event_id])
        })
        for all_gc in Backend._ref_counter[channel_name].values():
            if {event_id} & all_gc:
                seen = True
                continue
        if not seen:
            self.free(channel_name, event_id)


    def put(self, channel_name, event_id, event):
        Backend._db[channel_name].put(event_id, event)
        self.set_ref(channel_name, event_id)

    def get(self, channel_name,  event_id):
        return Backend._db.get(channel_name, {}).get(event_id)

    def free(self, channel_name, event_id):
        Backend._db[channel_name].invalidate(event_id)

    def drop(self, db):
        del Backend._db[db]


if '__main__' == __name__:
    CONFIG, LOCAL_INFO, ID = construct_info(sys.argv, "tracker")
    dictConfig(CONFIG.get("logging",{}))
    log = logging.getLogger("tracker")
    CONFIG.update(LOCAL_INFO)
    D = log.debug

    D("Started tracker")

    from dogpile.cache.region import make_region
    def cache_maker(name):
        reg= make_region(
            ).configure( 'dogpile.cache.dbm',
                expiration_time = 30,
                arguments = {
                    "filename":"./%s.dbm" % name
                })
        reg.put = reg.set
        reg.invalidate = reg.delete
        return reg
    task_storage = Backend(cache_maker = cache_maker)
    atexit.register(lambda : task_storage.save("me"))
    def see(task_storage):
        for cons, channel in product(("cons1", "cons2"), ("ch1","ch2")):
            if cons in task_storage._ref_counter[channel]:
                print "*" * 50
                print "CHANNEL %s CONS %s" % (channel, cons)
                print task_storage._ref_counter[channel][cons]
                print ""
    try:
        task_storage.load("me")
        for i in range(10):
            print "%d %r" % (i, task_storage.get("ch1", str(i)))
        task_storage.sync()
        see(task_storage)
    except Exception as e:
        print "NO BACKUP (%r) loaded" % e
        if e:
            raise e
        pass
    del(task_storage)
    task_storage = Backend(cache_maker = cache_maker)


    task_storage.register("ch1", "cons1")
    task_storage.register("ch1", "cons2")
    task_storage.register("ch2", "cons1")
    for i in range(10):
        task_storage.put("ch1",str(i) ,dict(channel="ch1", task=str(i), prod= 2 ))
        if i % 2:
            task_storage.put("ch2",str(i+10),dict(channel="ch2", task=str(i), prod= 1 ))

    see(task_storage)
    ### some random dec ref
    ch1_before_all_union = set(reduce(set.__or__,task_storage._ref_counter["ch1"].values()))
    ch2_before_all_union = set(reduce(set.__or__,task_storage._ref_counter["ch2"].values()))



    for i in range(5):
        k= str(i)
        print "decref ch1 cons1 %d" % i
        task_storage.dec_ref("ch1","cons1" , str(i) )
        print "decref ch1 cons2 %d" % (i+2)
        task_storage.dec_ref("ch1","cons2" , str(i+2))
        # [1, 2] + [6, 8] deref 1ce
        # [ 3, 4 , 5 ] on ch1 should be out
    deref_once = { 1, 2 , 6, 8}
    deref_twice = { 3, 4, 5 }
    ch1_after_all_union = set(reduce(set.__or__,task_storage._ref_counter["ch1"].values()))
    ch2_after_all_union = set(reduce(set.__or__,task_storage._ref_counter["ch2"].values()))


    def nb_ref(task_storage, channel, task_id):
       return { cons: int(task_id in gc) for cons,gc in task_storage._ref_counter[channel].items()}

    for task_id in sorted(ch1_before_all_union):
        ok_there = ch1_before_all_union & ch1_after_all_union
        print "%r is %s,  %s %s" % (task_id,
            task_storage.get("ch1", str(task_id)) and "present" or "absent",
            task_id in ch1_after_all_union and "should be here" or "shouldn't be there",
            nb_ref(task_storage, "ch1", str(task_id)))
        print task_storage.get("ch1", str(task_id))
        
    #task_storage.dec_ref(channel, lurker, new["task_id"])
    #task_storage.unregister(channel, new["emitter"])
