
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
    cfg.update(ctx.config)
    cfg.update(ctx.local_info)
    
    sock.connect((cfg['carbon']['server'], cfg['carbon']['port']))

    def send(measure_dict):
        if not measure_dict:
            return None
        try_me = 2
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



