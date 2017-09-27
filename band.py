from attach import *
import numpy as np

class source():
    def __init__(self,src,idx):
        self.src=src
        self.idx=idx

    def smooth(self,other):
        pass

    def chk(self,idx):
        pass


class puzzle():
    def __init__():
        pass

def overlap(st,nd):
    rst=[st[0],st[-1],nd[0],nd[-1]]
    rst.sort()
    return rst[1],rst[2]

def smooth(src,bg,ed):
    bg=np.where(src.idx==bg)
    ed=np.where(src.idx==ed)
    src.src[bg:ed,VOLUME]

if __name__=="__main__":
    a=np.arange('2005-02-01', '2005-03-01', dtype='datetime64[D]')
    b=np.arange('2005-02-20', '2005-03-20', dtype='datetime64[D]')
    print(overlap(a,b))
