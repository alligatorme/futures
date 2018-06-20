import pickle as pkl
import sqlite3, os

ROWS=['date','open','high','low','close','volume','open_interest','turnover','settle']

ROWS_TYPE=['DATE','REAL','REAL','REAL','REAL','INTEGER','INTEGER','INTEGER','REAL']
COLUMN_DEFINE=', '.join(list(' '.join(i) for i in zip(ROWS,ROWS_TYPE)))

class mbase():#metaclass=singleton
	def __init__(self,fn):
		if fn: self.base=sqlite3.connect(fn)
		self.tables=self.table_list()

	def table_list(self):
		rst=self.base.execute("select name from sqlite_master where type='table' order by name")
		return [i[0] for i in rst] 

	def row_in(self,symbol,row):
		sql="insert into %s (%s) values (%s)"%(symbol,','.join(ROWS),','.join(['?' for i in range(len(ROWS))]))
		self.base.execute(sql,row)

	def create_table(self,symbol):
		if symbol not in self.tables:
			sql="create table if not exists %s (%s)"%(symbol,COLUMN_DEFINE+', PRIMARY KEY (date)')
			self.base.execute(sql)
			self.table.append(symbol)

	def day_in(self,src):
		for i in src.iterrows():
			row=list(i[1])
			self.base.create_table(row[0])
			self.base.row_in(row[0],row[1:])

	def leave(self):
		self.base.close()

if __name__=="__main__":
	sd=mbase('temp.db')
	import pandas as pd
	from tst import get_var
#		sd.leave()
#	day_in('',get_var())
	ab=['symbol']
	ab.extend(ROWS)
#	sd.day_in(get_var()[ab])
	print(sd.table_list())
	sd.leave()
