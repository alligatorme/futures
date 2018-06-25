import sqlite3,datetime,os
import taken
import pandas as pd
import numpy as np

def row_new(dst,src):
	dst=np.array(dst)
	if not np.any(src-dst):
		return False
	else:
		return True
def row_valid(src):
#	if isinstance(src,list):src=np.array(src)
#	return np.all(src>=0)
	return True

ROWS=['date','open','high','low','close','volume','open_interest','turnover','settle']

ROWS_TYPE=['DATE','REAL','REAL','REAL','REAL','INTEGER','INTEGER','INTEGER','REAL']
COLUMN_DEFINE=', '.join(list(' '.join(i) for i in zip(ROWS,ROWS_TYPE)))

col=lambda x:','.join(x)
col_mrk=lambda x:','.join(['?' for i in range(len(x))])

class mbase():#metaclass=singleton
	def __init__(self,fn):
		if fn: self.base=sqlite3.connect(fn)
		self.get_tables()

	def get_tables(self):
		rst=self.base.execute("select name from sqlite_master where type='table' order by name")
		self.tables=[i[0] for i in rst]

	def create_table(self,symbol,year):
		if not symbol[-4].isdigit():
			print(symbol,'=',end='')
#			year=datetime.datetime.strptime(str(year),'%Y%m%d').year()
			year,_=divmod(year,10000)
			while str(year)[-1]!=symbol[-3]: year+=1
			symbol=symbol[:-3]+str(year)[-2:]+symbol[-2:]
			print(symbol)
		if not symbol in self.tables:
			sql="create table if not exists %s (%s)"%(symbol,COLUMN_DEFINE+', PRIMARY KEY (date)')
			self.base.execute(sql)
			self.tables.append(symbol)
		return symbol

	def day_in(self,src):
		for i in src.iterrows():
			row=list(i[1])
#			self.create_table(row[0],row[1])
			self.row_update(row[0],row[1:])

	def row_update(self,symbol,row):
		symbol=self.create_table(symbol,row[0])
		sql="select %s from %s where date=%s"%(col(ROWS[1:]),symbol,row[0])
		rst=self.base.execute(sql).fetchone()
		if not rst or row_new(list(rst),row[1:]):
			sql="replace into %s (%s) values (%s)"%(symbol,col(ROWS),col_mrk(ROWS))
			self.base.execute(sql,row)
			if rst:print(symbol,'=',row[0])
#			return True
#		else:
#			return False

	def inactive(self,threshold):
		for i in self.tables:
			sql="select volume from %s"%(i)
			rst=self.base.execute(sql).fetchall()
			rst=max(rst[0])
			if rst<threshold:print(i,'=',rst)

	def leave(self):
		self.base.commit()
		self.base.close()

	def clean_table(self):
		for i in self.tables:
			self.base.execute("drop table %s"%(i))
		self.base.execute("vacuum")

def main03():
	sd.get_tables()
	tk=taken.market()
	ab=['symbol']
	ab.extend(ROWS)
	for j in tk.raw_daily('2018-06-11'):
		print(20*'*')
		if isinstance(j,pd.core.frame.DataFrame):
			for i in j.iterrows():
				print(list(i[1])[0],end='\t')

def main02():
#	sd.row_update(['A1901','20180621'])
#	print(rst)
	sd.inactive(10000)

def main01():
	sd.clean_table()
	sd.get_tables()
	tk=taken.market()
	ab=['symbol']
	ab.extend(ROWS)
	for i in taken.serial_date(end='2018-06-21'):
		print(i)
		for j in tk.raw_daily(i):
			if isinstance(j,pd.core.frame.DataFrame):
				sd.day_in(j[ab])
#				print(j.head(1))

if __name__=="__main__":
	sd=mbase('temp.db')
	main01()
#	main02()
#	main03()
	sd.leave()
