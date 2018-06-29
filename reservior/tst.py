import pandas as pd
import mbase,taken
import pickle as pkl
def save_var(n,v):
	with open(n,'wb') as fd:
		pkl.dump(v,fd)

def main03():
	pass

def main02():
	sql="select volume from %s"%('A1901')
	rst=sd.raw.execute(sql).fetchall()
	rst=[i[0] for i in rst]
#	sd.row_update(['A1901','20180621'])
	print(rst)
#	sd.inactive(10000)

def main01():
#	sd.clean_table()
#	sd.get_tables()
	tk=taken.market()
	ab=['symbol']
	ab.extend(mbase.ROWS)
	for i in taken.serial_date(begin='2018-06-27',end='2008-01-01'):
		print(i)
		rst=[]
		for j in tk.raw_daily(i):
			if isinstance(j,pd.core.frame.DataFrame):rst.append(j)
		if rst:save_var(i,pd.concat(rst))
#				sd.day_in(j[ab])
#				print(j.head(1))
#				for i in j.iterrows():
#					print(list(i[1])[0],end='\t')

if __name__=="__main__":
	sd=mbase.mbase('raw.db')
	main01()
#	main02()
#	main03()
	sd.leave()
