import configparser
import os,sqlite3
from numpy import mean as avg

MARK='Dominant'

class info(configparser.ConfigParser):

	def __init__(self,raw='raw.db',fine='fine.db',cfg='cfg.ini'):
		configparser.ConfigParser.__init__(self)
		if os.path.exists(cfg):
			self.read(cfg)
		self.cfg=cfg
		self.raw=sqlite3.connect(raw)
#		self.fine=sqlite3.connect(fine)

	def init_config(self):
		rst=self.raw.execute("select name from sqlite_master where type='table' order by name")
		self.tables=[i[0] for i in rst]
		rst=set([i[:-4] for i in self.tables])
		if not self.has_section(MARK):self.add_section(MARK)
		for i in rst:
			if not i in self[MARK]:self.set(MARK,i,'')

	def division(self,symbol):
		rst=[j for j in self.tables if j[:-4]==symbol]
		tsr={}
		for j in rst:
			mk=j[-2:]
			if mk in tsr:
				tsr[mk].append(j)
			else:
				tsr[mk]=[j]
		return tsr

	def pick(self,symbol):
		wt={}
		for k,v in self.division(symbol).items():
			tsr=[]
			for i in v:
				sql="select volume from %s"%(i)
				rst=self.raw.execute(sql).fetchall()
				tsr.extend([i[0] for i in rst])
			wt[k]=avg(tsr)
#		for k,v in sorted(wt.items(),key=lambda d:d[1],reverse=True):
#			print(k,'=',v)
		rst=[k for k,v in sorted(wt.items(),key=lambda d:d[1],reverse=True)]
		return rst

	def dominant(self,symbol=[]):
		if not symbol:symbol=self.options(MARK)
		if not isinstance(symbol,list):symbol=[symbol]
		for i in symbol:
			print(i,30*'*')
			self.set(MARK,i,','.join(self.pick(i)))

	def optionxform(self,optionstr):
		return optionstr

	def save(self):
		self.write(open(self.cfg,'w'))

if __name__=="__main__":
	cf=info()
#	cf.add_section('hd')
#	cf.set('hd','M','01,05,08')
	cf.init_config()
	cf.dominant()
	cf.save()
