import pandas as pd
import mbase,taken
import pickle as pkl
import requests,os
#import entity

def save_var(n,v):
	with open(n,'wb') as fd:
		pkl.dump(v,fd)

def xfer(x):
#	if re.match('\d{4}-\d{2}-\d{2}',x):
	if '-' in x:
		x=x[:4]+x[5:7]+x[-2:]
		return int(x)
	if '.' in x:return float(x)
	return int(x)

def single(symbol):
#	symbol='A0701'
	url='http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesDailyKLine?symbol='+symbol
	try:
		rst=requests.get(url).json()
		if rst:
			print(symbol+' is OK!')
			return [list(map(xfer,i)) for i in rst]
		else:
			print(symbol+' error:')
	except requests.RequestException as e:
		print(symbol+' error:')
		print(e)
		return None

def sina():
	import entity
	info=entity.info()
	drt='/home/raptor/rsvr/sina/'
	for k,v in info.items('Dominant'):
		for nod in v.split(',')[:3]:
			for i in range(19,-1,-1):
				symbol=k+str(i).zfill(2)+nod
				if not symbol in os.listdir(drt):
					rst=single(symbol)
					save_var(drt+symbol,rst)



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
	for i in taken.serial_date(begin='2013-02-21',end='2008-01-01'):
		print(i)
		rst=[]
		for j in tk.raw_daily(i):
			if isinstance(j,pd.core.frame.DataFrame):rst.append(j)
		if rst:save_var(i,pd.concat(rst))
#				sd.day_in(j[ab])
#				print(j.head(1))
#				for i in j.iterrows():
#					print(list(i[1])[0],end='\t')

def main04():
	sd.rows=sd.rows[:6]
	sd.rowstype=sd.rowstype[:6]
	sd.clean_table()
	sd.get_tables()
	tk=taken.market()
#	ab=['symbol']
#	ab.extend(mbase.ROWS)
	for symbol,j in tk.sina_file():
		if isinstance(j,list):
			sd.symbol_in(symbol,j)

if __name__=="__main__":
	sd=mbase.mbase('sina.db')
	main04()
#	main01()
#	sina()
#	print(single('A0201'))
#	main02()
#	main03()
	sd.leave()
