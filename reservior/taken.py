import tushare as ts
import datetime

import pickle as pkl
def save_var(v):
	with open('var.pkl','wb') as fd:
		pkl.dump(v,fd)

def get_var():
	with open('var.pkl','rb') as fd:
		tp=pkl.load(fd)
	return tp

def serial_date(end,begin=None):
	day=begin and datetime.datetime.strptime(begin,'%Y-%m-%d') or datetime.datetime.now()
	end=datetime.datetime.strptime(end,'%Y-%m-%d')
	while day>=end:
		if day.weekday()-5<0:
			yield day.strftime('%Y-%m-%d')
		day-=datetime.timedelta(days=1)


class market():
	def __init__(self):
		self.src={'dce':'future','shfe':None,'czce':'future'}

	def raw_daily(self,day):
		for k,v in self.src.items():
			factor={}
			factor['date']=day
			if v: factor['type']=v
			func=ts.__dict__['get_'+k+'_daily']
			yield func(**factor)

if __name__=="__main__":
	for i in serial_date():
		print(i)
