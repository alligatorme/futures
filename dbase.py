import sqlite3,os
import pandas as pd
from attach import elapse,singleton


class cbase(metaclass=singleton):	
	def __init__(self,dname):
		self.freq='1D'
		self.data={}
		self.dbase=sqlite3.connect(dname) #how about class destroy,the dbase.close

	# def get_freq(self):
	# 	return self._freq
	# def set_freq(self,freq):
	# 	self._freq=freq
	# 	self.table_name=self.name #+'_'+self._freq
	# freq=property(get_freq,set_freq)

	# @elapse
	def load_data(self,tab,prid,clms):
		para={'fr':prid[0],'to':prid[1]}
		# print(para)
		sql='select dt,'+','.join(clms)+' from '+tab+' where dt>=@fr and dt<=@to'
		return pd.read_sql(sql,self.dbase,index_col='dt',parse_dates=['dt'], params=para)

	@elapse
	def merge_data(self,tab,prid,clms):
		# print(prid)
		if tab not in self.data.keys() or self.data[tab].empty:#isempty(self.data[tab])
			self.data[tab]=self.load_data(tab,prid,clms)
		else: # add rows
			cdata=self.data[tab]
			span=[cdata.index[0].to_pydatetime(),cdata.index[-1].to_pydatetime()]
			
			clmn=cdata.columns.tolist()
			pre_frame=self.load_data(tab,[prid[0],span[0]],clmn) if prid[0]<span[0] else None
			pos_frame=self.load_data(tab,[span[1],prid[1]],clmn) if span[1]<prid[1] else None
			self.data[tab]=pd.concat([pre_frame,cdata,pos_frame])
		# add columns
		cdata=self.data[tab]
		span=[cdata.index[0].to_pydatetime(),cdata.index[-1].to_pydatetime()] if len(cdata)>0 else None
		clms=list(set(clms)-set(cdata.columns.tolist()))
		# print(clms)
		if clms!=[]:
			self.data[tab]=pd.concat([cdata,self.load_data(tab,span,clms)], axis=1)
	

if __name__=="__main__":
	a=cbase(os.path.split(os.path.realpath(__file__))[0]+'\SZD.s3db')
	s=['op', 'hi', 'lo', 'cl', 'vl']
	
	a.merge_data('DLm01',['2006-01-04','2015-12-31'],clms=s)
	
	print(a.data['DLm01'])