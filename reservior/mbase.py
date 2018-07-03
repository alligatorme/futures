import sqlite3,datetime,os
import pandas as pd
import numpy as np

def row_new(dst,src):
	rst=[dst[i]-src[i] for i in range(len(dst))]
	if not np.any(rst):
		return False
	else:
		return True

ROWS=['date','open','high','low','close','volume','open_interest','turnover','settle']
ROWS_TYPE=['DATE','REAL','REAL','REAL','REAL','INTEGER','INTEGER','INTEGER','REAL']

col=lambda x:','.join(x)
col_def=lambda x,y:', '.join(list(' '.join(i) for i in zip(x,y)))
col_mrk=lambda x:','.join(['?' for i in range(len(x))])

class mbase():#metaclass=singleton

	def __init__(self,raw):
		if raw: self.raw=sqlite3.connect(raw)
		self.rows=ROWS
		self.rowstype=ROWS_TYPE
		self.get_tables()

	def get_tables(self):
		rst=self.raw.execute("select name from sqlite_master where type='table' order by name")
		self.tables=[i[0] for i in rst]

	def create_table(self,symbol,year):
		if not symbol[-4].isdigit():
			year=divmod(year,10000)[0]
			rst={str(i)[-1]:str(i)[-2:] for i in [year,year+1,year+2]}
			symbol=symbol[:-3]+rst[symbol[-3]]+symbol[-2:]
		if not symbol in self.tables:
			sql="create table if not exists %s (%s)"%(symbol,col_def(self.rawself.rawstype)+', PRIMARY KEY (date)')
			self.raw.execute(sql)
			self.tables.append(symbol)
		return symbol

	def day_in(self,src):
		for i in src.iterrows():
			row=list(i[1])
			if row[0][-3:].isdigit():self.row_update(row[0],row[1:])
		self.raw.commit()

	def row_update(self,symbol,row):
		symbol=self.create_table(symbol,row[0])
		sql="select %s from %s where date=%s"%(col(self.rows[1:]),symbol,row[0])
		rst=self.raw.execute(sql).fetchone()
		if not rst or row_new(list(rst),row[1:]):
			sql="replace into %s (%s) values (%s)"%(symbol,col(self.rows),col_mrk(self.rows))
			self.raw.execute(sql,row)
			if rst:print(symbol,'=',row[0])

	def leave(self):
		self.raw.commit()
		self.raw.close()

	def clean_table(self):
		for i in self.tables:
			self.raw.execute("drop table %s"%(i))
		self.raw.execute("vacuum")

if __name__=="__main__":
	pass
#	sd=mbase('info.db')
#	ROWS=ROWS[:6]
#	print(ROWS)
#	sd.leave()
