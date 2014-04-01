"""
Using a monotonic time that does not skew for python < 3.2 on linux

Hum I should rewrite this code for python3 maybe. Maybe packaging works now?

"""

### Author: Armin Ronacher http://stackoverflow.com/users/19990/armin-ronacher 
### Modified Jtayon
### http://stackoverflow.com/a/1205762/1458574
### License : http://creativecommons.org/licenses/by-sa/3.0/
### Additionnal source


## MOD using a non affected by ntp time function (CLOCK_MONOTONIC_RAW)
### man clock_getres
#  CLOCK_MONOTONIC
#         Clock that cannot be set and represents monotonic time since some
#         unspecified starting point.
#
#  CLOCK_MONOTONIC_RAW (since Linux 2.6.28; Linux-specific)
#         Similar to CLOCK_MONOTONIC, but provides access to a raw
#         hardware-based time that is not subject to NTP adjustments.

#
## MOD using ctypes.byRef() 
#
#ctypes.pointer(obj)
#
#    This function creates a new pointer instance, pointing to obj. The returned
#object is of the type POINTER(type(obj)).
#
#    Note: If you just want to pass a pointer to an object to a foreign function
# call, you should use byref(obj) which is much faster.


__all__ = ["m_time", "original"]

import ctypes, os
### here check if it is linux else retrun time for windows
### else return not implemented error for anything else than linux
### I cross my finger linux don't change their API soon. 

CLOCK_MONOTONIC_RAW = 4 # see <linux/time.h>

class timespec(ctypes.Structure):
    _fields_ = [
        ('tv_sec', ctypes.c_long),
        ('tv_nsec', ctypes.c_long)
]

librt = ctypes.CDLL('librt.so.1', use_errno=True)

clock_gettime = librt.clock_gettime
clock_gettime.argtypes = [ctypes.c_int, ctypes.POINTER(timespec)]

def m_time():
    t = timespec()
    if clock_gettime(CLOCK_MONOTONIC_RAW, ctypes.byref(t)) != 0:
        errno_ = ctypes.get_errno()
        raise OSError(errno_, os.strerror(errno_))
    return t.tv_sec + t.tv_nsec * 1e-9

def original():
    t = timespec()
    if clock_gettime(CLOCK_MONOTONIC_RAW, ctypes.pointer(t)) != 0:
        errno_ = ctypes.get_errno()
        raise OSError(errno_, os.strerror(errno_))
    return t.tv_sec + t.tv_nsec * 1e-9



