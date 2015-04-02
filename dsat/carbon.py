from __future__ import absolute_import
import socket
from time import time

DEF_CFG=dict(
    carbon =dict(
        host = '127.0.0.1',
        port = 2003,
        retry = True,
    )
)

def carbon_maker(**options):
    sock= socket.socket()
    cfg = DEF_CFG.copy()
    cfg.update(options)

    def connect():
        try:
            sock.connect((cfg['carbon']['host'], cfg['carbon']['port']))
        except Exception as e:
            if cfg.get("living_dangerously", True):
                print "Exception %r caught, desactivating carbon" % e
                print "returning dummy function instead of sent"
                return lambda *a, **kw: True
            else:
                raise e
    connect()
    def send(measure_dict):
        if not measure_dict:
            return None
        try_me = 1
        timestp = time()
        # https://github.com/graphite-project/carbon/blob/master/lib/carbon/protocols.py#L77
        message = '\n'.join(["%s %.15f %f" % (
            ".".join([ "DSAT", cfg['name'] ,  cfg["where"], cfg["step"], path]),
                    1.0 * value, timestp) for \
                        path, value in measure_dict.items() ]
            ) + '\n'
        last_excp = None
        while try_me:
            try:
                sock.send(message)
            except Exception as last_excp:
                ## TODO intercept only connection error
                sock.close()
                connect()
                if not try_me:
                    raise("%r for %r" % (last_excp, cfg))
            try_me -= 1
        return not try_me
    return send


if "__main__" == __name__:
    from time import sleep
    for i in range(10):
        carbon_maker(name="test",**dict(carbon=dict(
            host="127.0.0.1", port=2003, living_dangerously = True),
            where = "here",
            step = "test",
            )
        )(dict( path = 1))
        sleep(1)
    print "ok"

