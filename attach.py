import time,datetime
from functools import wraps
import numpy as np

OPEN=0
HIGH=1
LOW=2
CLOSE=3
VOLUME=4

# @functools.wraps
def elapse(func):
	@wraps(func)
	def wrapped(*arg,**kw):
		start=time.clock()
		tmp=func(*arg,**kw)
		delta=time.clock()-start
		ut='s'
		times=1
		if delta<1:
			ut='ms'
			times=1000
		if delta<0.001:
			ut='us'
			times=1000000
		print('Time Cost of %s = %f %s' % (func.__name__,delta*times,ut))
		return tmp
	return wrapped


class singleton(type):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__cache = {} #weakref.WeakValueDictionary()

    def __call__(self, *args):
        cname=args[0]
        # print(cname)
        if cname not in self.__cache:
            obj = super().__call__(*args)
            self.__cache[cname] = obj
        return self.__cache[cname]

def shift(rst,n,dft=np.nan):
    na=np.zeros(abs(n))
    na[:]=dft
    if n>0:
        return np.append(na,rst[:-n])
    else:
        return np.append(rst[-n:],na)

class source():
    def __init__(self,src,idx):
        self.src=src
        self.idx=idx

    def union(self,other):
        pass

    def chk(self,idx):
        pass

       
