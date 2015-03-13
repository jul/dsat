
from __future__ import absolute_import
import socket
from time import time

DEF_CFG=dict(
    carbon =dict(
        server = '127.0.0.1',
        port = 2003,
    )
)
    
def carbon_maker(ctx, **options):
    sock=sock = socket.socket()
    cfg = DEF_CFG.copy()
    cfg.update(options)
    try: 
        sock.connect((cfg['carbon']['server'], cfg['carbon']['port']))
    except Exception as e:
        if cfg.get("living_dangerously", True):
            print "Exception %r caught, desactivating carbon" % e
            print "returning dummy function instead of sent"
            return lambda *a, **kw: True
        else:
            raise e

    def send(measure_dict):
        if not measure_dict:
            return None
        try_me = 1
        timestp = int(time())
        message = '\n'.join(["%s %s %s" % (
            ".".join([ "DSAT",  cfg["name"], cfg["where"], cfg["step"], path]), 
                    value, str(timestp)) for \
                        path, value in measure_dict.items() ]
            ) + '\n'
        last_excp = None
        while try_me:
            try:
                sock.send(message)
            except Exception as last_excp:
                ## TODO intercept only connection error
                sock.close()
                sock.connect((cfg['carbon']['server'], cfg['carbon']['port']))
                raise("%r for %r" % (last_excp, cfg))
            try_me -= 1
        return not try_me
    return send


if "__main__" == __name__:
    carbon_maker(dict(carbon=dict(
        host="127.0.0.1", port=2003, living_dangerously = True),
        where = "here",
        step = "test",
        )
    )(dict( path = 1))
    print "ok"

