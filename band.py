from attach import *
import numpy as np


class puzzle():
    def __init__():
        pass

def overlap(st,nd):
    rst=[st[0],st[-1],nd[0],nd[-1]]
    rst.sort()
    return rst[1],rst[2]

@elapse
def smooth(st,nd):
    # st=1st|nd=2nd|ed=end|bg=begin
    loc=lambda i,j: np.where(i.idx==j)
    bg,ed=overlap(st.idx,nd.idx)
    stc=np.cumsum(st.src[loc(st,bg):loc(st,ed),VOLUME])
    ndc=np.cumsum(nd.src[loc(nd,bg):loc(nd,ed),VOLUME])
    n=np.argmax(np.absolute(stc-ndc))+1
    t=len(stc)-n
    src=np.concatenate((st.src[:-t],nd.src[n:]))
    idx=np.concatenate((st.idx[:-t],nd.idx[n:]))
    return source(src,idx)

    
    
if __name__=="__main__":
    a=np.arange('2005-02-01', '2005-03-01', dtype='datetime64[D]')
    b=np.arange('2005-02-20', '2005-03-20', dtype='datetime64[D]')
    print(overlap(a,b))
