import tushare as ts
# print(type(ts.__dict__))
# for k,v in ts.__dict__.items():
# 	print(k,v)
import pickle as pkl

 
def save_var(v):
   with open('var.pkl','wb') as fd:
       pkl.dump(v,fd)

def get_var():
   with open('var.pkl','rb') as fd:
       tp=pkl.load(fd)
   return tp

save_var(ts.get_dce_daily(date='2018-06-06', type="future"))
print(get_var())
# print(ts.get_dce_daily(date='2018-06-06', type="future"))
# print(ts.get_shfe_daily(date='2018-06-06'))