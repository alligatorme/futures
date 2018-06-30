import tushare as ts
import datetime,os,re

import pickle as pkl
def save_var(v,n='var.pkl'):
	with open('var.pkl','wb') as fd:
		pkl.dump(v,fd)

def get_var(n='var.pkl'):
	with open(n,'rb') as fd:
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
#		self.src={'shfe':None,'dce':'future','czce':'future'}
		self.src={'dce':'future','czce':'future','shfe':None}

	def raw_daily(self,day):
		for k,v in self.src.items():
			factor={}
			factor['date']=day
			if v: factor['type']=v
			func=ts.__dict__['get_'+k+'_daily']
			yield func(**factor)

	def raw_file(self,drt=None):
		for i in sorted(os.listdir(drt)):
			if isinstance(i,str) and re.match('\d{4}-\d{2}-\d{2}',i):
				print(i)
				yield get_var(drt+'/'+i)

if __name__=="__main__":
#	for i in serial_date():
#		print(i)
#	print(get_var())
	mkt=market()
	for i in mkt.raw_daily('2018-06-26'):
		print(i)
