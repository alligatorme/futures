import sqlite3,datetime,os
import pandas as pd
import numpy as np
import configparser

class cfg(configparser.ConfigParser):
	def __init__(self,default=None):
		configparser.ConfigParser.__init__(self,default=None)
	def optionxform(self,optionstr):
		return optionstr

def row_new(dst,src):
	rst=[dst[i]-src[i] for i in range(len(dst))]
	if not np.any(rst):
		return False
	else:
		return True

ROWS=['date','open','high','low','close','volume','open_interest','turnover','settle']

ROWS_TYPE=['DATE','REAL','REAL','REAL','REAL','INTEGER','INTEGER','INTEGER','REAL']
COLUMN_DEFINE=', '.join(list(' '.join(i) for i in zip(ROWS,ROWS_TYPE)))

col=lambda x:','.join(x)
col_mrk=lambda x:','.join(['?' for i in range(len(x))])

class mbase():#metaclass=singleton

	def __init__(self,raw,info):
		if raw: self.raw=sqlite3.connect(raw)
		if info: self.info=sqlite3.connect(info)
		self.get_tables()

	def get_tables(self):
		rst=self.raw.execute("select name from sqlite_master where type='table' order by name")
		self.tables=[i[0] for i in rst]

	def get_category(self):
		self.category=[]
		for i in self.tables:
			if i[:-4] in self.category:
				self.category.append(i[:-4])

	def create_table(self,symbol,year):
		if not symbol[-4].isdigit():
			year,_=divmod(year,10000)
			while str(year)[-1]!=symbol[-3]: year+=1
			symbol=symbol[:-3]+str(year)[-2:]+symbol[-2:]
		if not symbol in self.tables:
			sql="create table if not exists %s (%s)"%(symbol,COLUMN_DEFINE+', PRIMARY KEY (date)')
			self.raw.execute(sql)
			self.tables.append(symbol)
		return symbol

	def day_in(self,src):
		for i in src.iterrows():
			row=list(i[1])
			self.row_update(row[0],row[1:])

	def row_update(self,symbol,row):
		symbol=self.create_table(symbol,row[0])
		sql="select %s from %s where date=%s"%(col(ROWS[1:]),symbol,row[0])
		rst=self.raw.execute(sql).fetchone()
		if not rst or row_new(list(rst),row[1:]):
			sql="replace into %s (%s) values (%s)"%(symbol,col(ROWS),col_mrk(ROWS))
			self.raw.execute(sql,row)
			if rst:print(symbol,'=',row[0])

	def inactive(self,threshold):
		for i in self.tables:
			sql="select volume from %s"%(i)
			rst=self.raw.execute(sql).fetchall()
			rst=max(rst[0])
			if rst<threshold:print(i,'=',rst)

	def leave(self):
		self.raw.commit()
		self.raw.close()

	def clean_table(self):
		for i in self.tables:
			self.raw.execute("drop table %s"%(i))
		self.raw.execute("vacuum")

if __name__=="__main__":
	pass
#	sd=mbase('temp.db','info.db')
#	sd.leave()
