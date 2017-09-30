from attach import *
import numpy as np
from dbase import cbase
import os,sqlite3


class puzzle():
    def __init__():
        pass

def overlap(st,nd):
    # st=1st|nd=2nd|ed=end|bg=begin
    loc=lambda i,j: np.where(i.idx==j)
    a1,a2,b1,b2=st.idx[0],st.idx[-1],nd.idx[0],nd.idx[-1]
    if a1<b1<a2:
        bg=loc(st,b1)[0][0]
        ed=loc(nd,a2)[0][0]
        for i,j in zip(st.idx[bg:],nd.idx[:ed]):
            print(i,j)
        stc=np.cumsum(st.src[bg:,VOLUME])
        ndc=np.cumsum(nd.src[:ed,VOLUME])
        print(len(stc),len(ndc))
        n=np.argmax(np.absolute(stc-ndc))+1
        print(n)
        return [n-len(stc),n]


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
    s=['op', 'hi', 'lo', 'cl', 'vl']    
    db.merge_data('DLa0801',['2006-07-26','2008-01-15'],clms=s)
    db.merge_data('DLa0805',['2006-11-15','2008-05-16'],clms=s)
    db.merge_data('DLa0809',['2007-03-20','2008-09-12'],clms=s)
    m1=source(db.data['DLa0801'].values,db.data['DLa0801'].index.values)
    m2=source(db.data['DLa0805'].values,db.data['DLa0805'].index.values)
    m3=source(db.data['DLa0809'].values,db.data['DLa0809'].index.values)
    mrk=smooth([m1,m2,m3])

    # import matplotlib.pyplot as plt
    # plt.plot(pta.plus)
    # plt.show()