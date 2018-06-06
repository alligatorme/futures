import tushare as ts
# print(type(ts.__dict__))
# for k,v in ts.__dict__.items():
# 	print(k,v)

# print(ts.get_czce_daily(date='2018-06-06', type="future"))
# print(ts.get_dce_daily(date='2018-06-06', type="future"))
print(ts.get_shfe_daily(date='2018-06-06'))