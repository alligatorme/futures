import sqlite3, os
import taken
import pandas as pd
import numpy as np

def row_new(dst,src):
	if not np.any(src-dst):return false
#		if np.all((src[1:]-rst)>=0):

def row_valid(src):
	if isinstance(src,list):src=np.array(src)
	return np.all(src>=0)

ROWS=['date','open','high','low','close','volume','open_interest','turnover','settle']

ROWS_TYPE=['DATE','REAL','REAL','REAL','REAL','INTEGER','INTEGER','INTEGER','REAL']
COLUMN_DEFINE=', '.join(list(' '.join(i) for i in zip(ROWS,ROWS_TYPE)))

class mbase():#metaclass=singleton
	def __init__(self,fn):
		if fn: self.base=sqlite3.connect(fn)
		self.get_tables()

	def get_tables(self):
		rst=self.base.execute("select name from sqlite_master where type='table' order by name")
		self.tables=[i[0] for i in rst]

	def create_table(self,symbol):
		if not symbol in self.tables:
			print(symbol)
			sql="create table if not exists %s (%s)"%(symbol,COLUMN_DEFINE+', PRIMARY KEY (date)')
			self.base.execute(sql)
			self.tables.append(symbol)

	def row_in(self,symbol,row):
		sql="insert into %s (%s) values (%s)"%(symbol,','.join(ROWS),','.join(['?' for i in range(len(ROWS))]))
		self.base.execute(sql,row)

	def day_in(self,src):
		for i in src.iterrows():
			row=list(i[1])
			self.create_table(row[0])
			self.row_in(row[0],row[1:])

	def row_update(self,src):
#		symbol=src[0]
#		date=src[1]
		col=lambda x:','.join(x)
		sql="select %s from %s where date=%s"%(col(ROWS[1:]),src[0],src[1])
		rst=self.base.execute(sql).fetchone()
		if row_new(list(rst),src[1:]):
			sql="replace into %s (%s) values (%s)"%(src[0],col(ROWS),','.join(['?' for i in range(len(col(ROWS)))]))
			self.base.execute(sql,src[1:])
			return true
		else:
			return false

	def leave(self):
		self.base.commit()
		self.base.close()

	def clean_table(self):
		for i in self.tables:
			self.base.execute("drop table %s"%(i))
		self.base.execute("vacuum")

def main02():
	sd.row_update(['A1901','20180621'])
#	print(rst)

def main01():
#	sd.clean_table()
#	sd.get_tables()
	tk=taken.market()
	ab=['symbol']
	ab.extend(ROWS)
	for i in taken.serial_date(end='2018-06-01'):
		print(i)
		for j in tk.raw_daily(i):
			if isinstance(j,pd.core.frame.DataFrame):
				sd.day_in(j[ab])
#				print(j.head(1))

if __name__=="__main__":
	sd=mbase('temp.db')
	main02()
	sd.leave()

