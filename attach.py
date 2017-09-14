import time,datetime
from functools import wraps

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
		print('Time Cost of %s = %f %s' % (func.__name__,delta*times,ut))
		return tmp
	return wrapped