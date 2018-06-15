import tushare as ts

import pickle as pkl
import datetime
# print(type(ts.__dict__))
# for k,v in ts.__dict__.items():
# 	print(k,v)

def save_var(v):
   with open('var.pkl','wb') as fd:
       pkl.dump(v,fd)

def get_var():
   with open('var.pkl','rb') as fd:
       tp=pkl.load(fd)
   return tp

#save_var(ts.get_dce_daily(date='2018-06-06', type="future"))
#print(get_var())
#print(ts.get_czce_daily(date='2018-06-06', type="future"))
# print(ts.get_shfe_daily(date='2018-06-06'))


# def serial_date(begin=None,end=datetime.datetime(2000, 1, 1)):
# 	day=begin or datetime.datetime.now()
# 	while day>=end:
# 		if day.weekday()-5<0:
# 			yield day.strftime('%Y-%m-%d')
# 		day=day-datetime.timedelta(days=1)

# x=serial_date(end=datetime.datetime(2018, 5, 2))
# for i in x: print(i)

def get_raw(day='2016-06-22'):
	src={'dce':'future','shfe':None,'czce':'future'}
	for k,v in src.items():
		factor={}
		factor['date']=day
		if v: factor['type']=v
		func=ts.__dict__['get_'+k+'_daily']
		print(k,'----')
		print(func(**factor).head())
get_raw()
