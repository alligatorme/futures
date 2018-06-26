import mbase,taken

def main03():
	pass

def main02():
#	sd.row_update(['A1901','20180621'])
#	print(rst)
	sd.inactive(10000)

def main01():
	sd.clean_table()
	sd.get_tables()
	tk=taken.market()
	ab=['symbol']
	ab.extend(mbase.ROWS)
	for i in taken.serial_date(end='2018-06-01'):
		print(i)
#		for j in tk.raw_daily(i):
#			if isinstance(j,pd.core.frame.DataFrame):
#				sd.day_in(j[ab])
#				print(j.head(1))
#				for i in j.iterrows():
#					print(list(i[1])[0],end='\t')

if __name__=="__main__":
	sd=mbase.mbase('temp.db','info.db')
	main01()
#	main02()
#	main03()
	sd.leave()
