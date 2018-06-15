import pickle as pkl
import sqlite3, os

def get_var():
   with open('var.pkl','rb') as fd:
       tp=pkl.load(fd)
   return tp

ROWS=['date','open','high','low','close','volume','open_interest','turnover','settle']

ROWS_TYPE=['DATE','INT','INT','INT','INT','INTEGER','INTEGER','INTEGER','INT']
ROWS_MARK=['?' for i in range(len(ROWS))]
COLUMN_DEFINE=','.join(list(' '.join(i) for i in zip(ROWS,ROWS_TYPE)))

class mbase():#metaclass=singleton
	def __init__(self,fn):
		if fn: self.base=sqlite3.connect(fn)

	def row_in(self,symbol,row):
		sql="insert into %s (%s) values (%s)"%(symbol,','.join(ROWS),','.join(ROWS_MARK)) 
		self.base.excute(sql,row)

	def create_table(self,symbol):
		sql="create table if not exists %s (%s)"%(symbol,COLUMN_DEFINE+', PRIMARY KEY (date)')

if __name__=="__main__":
	# print(ROWS)
	# print(ROWS_MARK)
	symbol='A1009'
	sql="create table if not exists %s (%s)"%(symbol,COLUMN_DEFINE+', PRIMARY KEY (date)')
	print(sql)
