from dbase import cbase
from attach import elapse
from collections import deque
import os,sqlite3
import numpy as np
import pandas as pd
import talib
# from talib.abstract import *
class singleton(type):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__cache = {} #weakref.WeakValueDictionary()

    def __call__(self, *args):
        cname=args[0]
        # print(cname)
        if cname in self.__cache:
            return self.__cache[cname]
        else:
            obj = super().__call__(*args)
            self.__cache[cname] = obj
            return obj

class account(metaclass=singleton):
    def __init__(self,name):
        self.name=name
    def open(self,ctr,t,n,p):
        contract(self.name+'|'+ctr).pos_op(t,n,p)
    def close(self,ctr,t,n,p):
        contract(self.name+'|'+ctr).pos_cl(t,n,p)

    def refresh(self):
        ref=contract._singleton__cache
        ctr=(ref[i] for i in ref.keys() if i.startswith(self.name+'|'))
        # for i in ctr:
        #     i.refresh()
        #     self.mrg+=i.mrg
        #     self.fri+=i.fri
        #     self.prf+=i.prf         

class contract(metaclass=singleton):
    def __init__(self,name):
        '''
            p:price|n:amount|rto:margin ratio|tax:fee|npr:amount per contract|drt:direction-open/short|prf:profit|mrg:margin|fri:fixed profit
        '''
        # self.tank={}
        self.tank=deque()
        self.fri=0
        self.pst=0
        self.avgp=0
        
        self.npr,self.tax,self.rto=get_info(self.get_name(name))

        # self.name=name
        # self.drt=(name[-1:]=='-') and -1 or 1
        self.get_prf=lambda n,p:n*(self.drt*self.npr*p-self.tax)
        self.get_mrg=lambda n,p:n*p*self.rto*self.npr/100    
     
    def get_name(self,name):
        name=name.split('|')[-1]
        print(name)
        self.drt=(name[-1:]=='-') and -1 or 1
        return name[:-1]

    def refresh(self,prs):
        # self.mrg=0
        self.prf=0
        for _,n,p in self.tank:
            self.prf+=self.get_prf(n,prs-p)
            # self.mrg+=self.get_mrg(n,prs)

    def lump(self):
        cum=cnt=0
        for _,n,p in self.tank:
            cum+=n*p
            cnt+=n
        self.pst=cnt
        self.avgp=cnt and cum/cnt or 0
        # return cnt and cum/cnt or 0

    def ocsc(self,idt,t,n,p):        
        if idt==self.drt:
            self.tank.append((t,n,p))
        else:
            # n1=n
            while (n>0 and len(self.tank)>0):
                t0,n0,p0=self.tank.pop()
                self.fri+=self.get_prf(min(n,n0),p-p0)
                n-=n0
                if n<0: self.tank.append((t0,-n,p0))
        # self.pst+=idt*self.drt*n
        self.lump()
    
    def divide(self,src,idx):
        # div=(src.get_loc(i) for i in idx)
        div=(np.where(src==i) for i in idx)
        start=0
        for ([i],) in div:
            yield (start,i)
            start=i
        yield (start,-1)

    @elapse
    def element(self,src,sign):
        sign_iter=np.nditer(sign.src)
        for i,j in self.divide(src.idx,sign.idx):
            src.plus[i:j]=self.get_prf(self.pst,src.src[i:j,3]-self.avgp)+self.fri
            if i==0: continue
            self.ocsc(sign_iter.__next__(),src.idx[i],1,src.src[i,3])


dname=os.path.split(os.path.realpath(__file__))[0]+'\me.s3db'
def get_info(name):    
    dbase=sqlite3.connect(dname)  
    cr=dbase.cursor()
    sql='select nper,rto,tax from Contract where name=\''+name+'\''
    print(sql)
    cr.execute(sql)
    rt=list(cr.fetchall()[0])
    dbase.close
    return rt
#*****************************************************************************************
def shift(rst,n):
    na=np.zeros(abs(n))
    na[:]=np.nan
    if n>0:
        return np.append(na,rst[:-n])
    else:
        return np.append(rst[-n:],na)

@elapse
def cross(rst):    
    rst=np.sign(rst)
    # print(rst)
    sft=shift(rst,1)
    # sft=rst.__rshift__(1)
    zr=rst==0
    if any(zr): 
        print('zero appears =',len(zr[zr]))
        rst[zr]=sft[zr]
    sft=shift(rst,1) 
    mrk=~np.logical_or(sft==rst,np.isnan(sft))
    mrk=np.ma.array(rst,mask=~mrk)
    # mrk=np.where(mrk)
    # print(mrk)
    return mrk


def stg(t):
    # print(t)
    if t.dtype!=float: t=t.astype('float')
    return talib.SMA(t,7)-talib.SMA(t,20)

class source():
    def __init__(self,src,idx):
        self.src=src
        self.idx=idx
        self.plus=np.zeros(len(src))

@elapse
def main02():
    pta=contract('m1505-')
    # pta.tax=0
    # df=db.data['m1505']
   
    m15=source(db.data['m1505'].values,db.data['m1505'].index.values)
    idx=cross(stg(m15.src.T[3]))
    # print(signal.mask)
    # print(df.index[~idx.mask][1:])
    # idx=pd.DataFrame(idx.compressed(),index=df.index[~idx.mask],columns=['signal'])
    idx=source(idx.compressed(),m15.idx[~idx.mask])
    ts=pta.element(m15,idx) 
    # print(m15.plus)

    import matplotlib.pyplot as plt
    plt.plot(m15.plus)
    plt.show()
  



@elapse
def main():
    a=cbase(os.path.split(os.path.realpath(__file__))[0]+'\me.s3db')
    s=['op', 'hi', 'lo', 'cl', 'vol']    
    a.merge_data('m1505',['2008-12-01 09:00:00','2008-12-28 23:00:00'],clms=s)
 
    t=np.array(a.data['m1505'].cl.values, dtype=float)
    # import matplotlib.pyplot as plt
    # plt.plot(t)
    # plt.plot(talib.SMA(t,7))
    # plt.plot(talib.SMA(t,20))
    # plt.show()
    # b=talib.SMA(t,7)-talib.SMA(t,20)
    b=stg(t)
    print(b)
    # print(b)
    signal=cross(b)

    # c=sma(b,n=5)
    print(len(b))


if __name__=="__main__":
    np.seterr(invalid='ignore')
    db=cbase(os.path.split(os.path.realpath(__file__))[0]+'\me.s3db')
    s=['op', 'hi', 'lo', 'cl', 'vol']    
    db.merge_data('m1505',['2008-12-01 00:00:00','2008-12-20 23:00:00'],clms=s)
    # main()
    main02()
