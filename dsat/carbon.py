from __future__ import absolute_import
import socket
from time import time
from sys import stderr

DEF_CFG=dict(
    carbon =dict(
        host = '127.0.0.1',
        port = 2003,
        retry = True,
        debug = False,
    )
)

def carbon_maker(**options):
    cfg = DEF_CFG.copy()
    cfg.update(options)

    def send(measure_dict):
        if not measure_dict:
            return None
        message = '\n'.join(["%s %.15f %f" % (
            ".".join([ "DSAT", cfg['name'] ,  cfg["where"], cfg["step"], path]),
                    1.0 * value, time()) for \
                        path, value in measure_dict.items() ]
        ) + '\n'
        try:
            sock= socket.socket()
            sock.connect((cfg['carbon']['host'], cfg['carbon']['port']))
            sock.sendall(message)
            sock.close()
        except Exception as last_excp:
            if cfg.get("debug"):
                print >>stderr, last_excp
            pass
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

