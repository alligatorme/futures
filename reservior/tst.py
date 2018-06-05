import tushare as ts
# print(type(ts.__dict__))
for k,v in ts.__dict__.items():
	print(k,v)

# print(ts.get_czce_daily(date='2017-01-05', type="future"))
