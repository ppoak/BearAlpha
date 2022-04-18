import pandas as pd

def index_dim(data: 'pd.DataFrame | pd.Series') -> int:
    '''return the index dimension number of given data'''
    try:
        dim = len(data.index.levshape)
    except:
        dim = 1
    return dim
