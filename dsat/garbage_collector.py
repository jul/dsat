#!/usr/bin/env pytho
# -*- coding: utf-8 -*-

from archery.bow import Daikyu as sdict
import sys
import os
import atexit



from logging.config import dictConfig
import logging

from itertools import islice, product
from dsat.state import construct_info, get_connection


#pyzmq is string agnostic, so we ensure we use bytes
import simplejson as json
from simplejson import loads, dumps


SENTINEL = object
HOUR = 3600
DAY = 24 * HOUR
#### let's 
#time_keeper = scheduler(time, sleep)
if not len(sys.argv) >= 1:
    raise( Exception("Arg"))

def warn(msg):
    if True:
        print msg
    else:
        raise(Exception(msg))

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
import os
import atexit
from dogpile.cache.region import make_region
from collections import deque
from simplejson import load, dump
class SerializedQueue(object):
    def __init__(self, **option):
        self.name = option["name"]
        self.load()
        self.cache = make_region(
            ).configure( 'dogpile.cache.dbm',
                ### 2 days
                expiration_time = 3600 * 7 * 24,
                arguments = {
                    "filename":"./%s.dbm" % option["name"]
                })
        atexit.register(lambda: self.save())
    def pop(self):
        task_id = self._queue.pop()
        vector = self.cache.get(str(task_id))
        return task_id, vector

    def append(self, task_id, vector):
        self._queue.append(str(task_id))
        self.cache.set(str(task_id), vector)

    def insert(self, task_id, vector):
        self._queue.appendleft(str(task_id))
        self.cache.set(str(task_id), vector)

    def load(self):
        if not os.path.exists(self.name):
            self._queue = deque()
            self.save()
        else:
            try:
                self._queue = deque(map(str,load(open("%(name)s" % self.__dict__ ))))
            except Exception as e:
                self._queue = deque()

    def save(self):
        dump(list(self._queue), open("%(name)s" % self.__dict__, "w"))

    def __len__(self):
        return len(self._queue)



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
    # The singleton approach I am trying to have is stupid.

    # On the other hand keeping the file for saving in O_EXCL could help 
    # Ensuring one access possible at a time ... as long as we are living
    # in a non distributed env
    _db = None
    # list of reference counts per channel per lurkers
    ## requires that keys are unique for EVERY region (hence the use of stamper)
    _ref_counter = sdict()
    atexit = None

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

    def register_atexit(self):
        if not Backend.atexit:
            atexit.register(lambda :self.save())
            Backend.atexit = True

    def save(self, name = SENTINEL):
        """on successfull load/save register the atexit save"""
        if name is SENTINEL:
            name = self.backup_name
            if not name:
                raise(Exception("What?!"))
        with open(name, "w") as save_there:
            json.dump(self._dump(), save_there)
            self.backup_name=name
            self.register_atexit()

    def load(self, name = SENTINEL):
        name = name is SENTINEL and self.backup_name or name
        if not name is SENTINEL and os.path.exists(name):
            with open(name) as backup :
                try:
                    self._load(json.load(backup))
                    self.backup_name=name
                    self.sync()
                except Exception as e:
                    logging.exception(e)
                    pass
        else:
            warn("No bakcup found")

    def sync(self):
        task_per_chan = sdict()
        for chan, gc in Backend._ref_counter.items():
            task_per_chan += sdict({ chan :ToxicSet( [ l for x in  gc.values() for l in x]) })
        new_ref_counter = Backend._ref_counter.copy()
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
        self.backup_name = option.get("name", 'garbage_collector')
        if Backend._db is None:
            Backend._db = sdict()
        if "cache_maker" not in option:
            from repoze.lru import LRUCache
        self.cache_maker = option.get("cache_maker",
            lambda name: LRUCache(1000000)
        )
        self.option = option
        self.backend_has_expiry_time = option.get("backend_has_expiry_time", True)

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
        if self.backend_has_expiry_time:
            Backend._db[channel_name].invalidate(event_id)

    def drop(self, db):
        #del Backend._db[db]
        pass

def dogpile_builder(name,dogpile_dict_config ={}, *a, **kw):
    """map entries of input to buid region according to dogpile
    then buid the Backend with kw
    dogpile_dict_config is made of
        * positionnal a driver name for the backend
        * named argument
    why not a tuple instead of a dict with one key? 
    It is easier to serialize/put in a config
    """
    from dogpile.cache.region import make_region
    DEFAULT_CONFIG =  {
            'dogpile.cache.dbm' : dict(
                    expiration_time = 2 * DAY,
                    arguments = { "filename":"./%s.dbm" % name }
                )
        }

    if not dogpile_dict_config or set(dogpile_dict_config) & set(DEFAULT_CONFIG):
        DEFAULT_CONFIG.update(dogpile_dict_config)
        dogpile_dict_config = DEFAULT_CONFIG

    def cache_maker(name):
        reg= make_region().configure(
                dogpile_dict_config.keys().pop(),
                **dogpile_dict_config.values()[0]
            )
        reg.put = reg.set
        reg.invalidate = reg.delete
        return reg
    CONFIG = dict(cache_maker = cache_maker, name = "%s.save" % name)
    CONFIG.update(kw)
    return Backend(**CONFIG)

if '__main__' == __name__:
    task_storage = dogpile_builder('file_test')
    def see(task_storage):
        for cons, channel in product(("cons1", "cons2"), ("ch1","ch2")):
            if cons in task_storage._ref_counter.get(channel, ()):
                print "*" * 50
                print "CHANNEL %s CONS %s" % (channel, cons)
                print task_storage._ref_counter[channel][cons]
                print ""
    try:
        task_storage.load()
        for i in range(10):
            print "%d %r" % (i, task_storage.get("ch1", str(i)))
        task_storage.sync()
        print "INITIAL VALUE"
        see(task_storage)
    except Exception as e:
        print "NO BACKUP (%r) loaded" % e
        pass
    del(task_storage)
    task_storage = dogpile_builder('file_test')
    task_storage.load()
    task_storage.register_atexit()
    print "EMPTY"

    task_storage.register("ch1", "cons1")
    task_storage.register("ch1", "cons2")
    task_storage.register("ch2", "cons1")
    for i in range(10):
        task_storage.put("ch1",str(i) ,dict(channel="ch1", task=str(i), prod= 2 ))
        if i % 2:
            task_storage.put("ch2",str(i+10),dict(channel="ch2", task=str(i), prod= 1 ))
    print task_storage._db
    print Backend._ref_counter
    see(task_storage)
    print "*" * 80
    ### some random dec ref
    ch1_before_all_union = set(reduce(set.__or__,task_storage._ref_counter.get("ch1", dict()).values() or {}))
    ch2_before_all_union = set(reduce(set.__or__,task_storage._ref_counter.get("ch2", {}).values() or {}))



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
    ch1_after_all_union = reduce(set.__or__,task_storage._ref_counter["ch1"].values())
    ch2_after_all_union = reduce(set.__or__,task_storage._ref_counter["ch2"].values())

    task_storage.save()
    task_storage.load()

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
    see(task_storage)
