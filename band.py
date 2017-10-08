from attach import *
import numpy as np
import pandas as pd
from dbase import cbase
import os,sqlite3
import talib


class puzzle():
    def __init__():
        pass

# def overlap(st,nd):
#     # st=1st|nd=2nd|ed=end|bg=begin
#     loc=lambda i,j: np.where(i.idx==j)[0][0]
#     a1,a2,b1,b2=st.idx[0],st.idx[-1],nd.idx[0],nd.idx[-1]
#     if a1<b1<a2:
#         bg=loc(st,b1)
#         ed=loc(nd,a2)
#         stc=st.idx[bg:]
#         ndc=nd.idx[:ed]
#         olc=np.union1d(stc,ndc)
#         # for i,j in zip(st.idx[bg:],nd.idx[:ed]):
#         #     print(i,j)
#         stc=np.cumsum(st.src[bg:,VOLUME])
#         ndc=np.cumsum(nd.src[:ed,VOLUME])
#         print(len(stc),len(ndc))
#         n=np.argmax(np.absolute(stc-ndc))+1
#         print(n)
#         return [n-len(stc),n]

def overlap(st,nd):
    loc=lambda i,j: np.where(i.idx==j)[0][0]
    a1,a2,b1,b2=st.idx[0],st.idx[-1],nd.idx[0],nd.idx[-1]
    if a1<b1<a2:
        bg=loc(st,b1)
        ed=loc(nd,a2)
        stc=pd.DataFrame(st.src[bg:,VOLUME],index=st.idx[bg:],columns=['st'])
        ndc=pd.DataFrame(nd.src[:ed,VOLUME],index=nd.idx[:ed],columns=['nd'])
        ovr=stc.join(ndc)
        ovr.fillna(method='ffill',inplace=True)
        ovr=ovr.st-ovr.nd
        
        # print(talib.SMA(ovr.values,7))
        plt.plot(talib.SMA(ovr.values,7),'x')
        plt.show()


@elapse
def smooth(part):
    mrk=[0]
    for i,j in zip(part[:-1],part[1:]):
        mrk.extend(overlap(i,j))
    mrk.extend(None)
    src=[]
    idx=[]
    for p,(i,j) in zip(part,zip(*([iter(mrk)]*2))):
        src.append(p.src[i:j])
        idx.append(p.idx[i:j])
    return source(np.concatenate(src),np.concatenate(idx))
    
    
if __name__=="__main__":
    # a=np.arange('2005-02-01', '2005-03-01', dtype='datetime64[D]')
    # b=np.arange('2005-02-20', '2005-03-20', dtype='datetime64[D]')
    # print(overlap(a,b))

    db=cbase(os.path.split(os.path.realpath(__file__))[0]+'\szd.s3db')
    clms=['op', 'hi', 'lo', 'cl', 'vl']
    mrk=[]    

    import matplotlib.pyplot as plt
    sp=['DLa1001','DLa1005','DLa1009','DLa1101','DLa1105','DLa1109']
    # ,'DLa1201','DLa1205','DLa1209','DLa1301','DLa1305','DLa1309'
    for i in sp:
        db.merge_data(i,['2008-01-01','2013-12-31'],clms=clms)
        sgl=source(db.data[i].values,db.data[i].index.values)
        mrk.append(sgl)
        plt.plot(sgl.idx[:-30],sgl.src[:-30,VOLUME])
        # plt.plot(sgl.idx,talib.SMA(sgl.src[:,VOLUME],17))
    # mrk=smooth(mrk)
    # plt.axhline(y=100000,ls='--')
    # plt.axhline(y=50000,ls='--')
    # plt.yscale('log')
    plt.show()