import pandas as pd
import mbase,taken


def main01():
	tk=taken.market()
	ab=['symbol']
	ab.extend(mbase.ROWS)
	for i in taken.serial_date(begin='2013-02-21',end='2008-01-01'):
		print(i)
		rst=[]
		for j in tk.raw_daily(i):
			if isinstance(j,pd.core.frame.DataFrame):rst.append(j)
		if rst:save_var(i,pd.concat(rst))


if __name__=="__main__":
	main01()
