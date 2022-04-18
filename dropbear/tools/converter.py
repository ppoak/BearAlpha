import re
import datetime
import pandas as pd
import numpy as np
from typing import Iterable


def datetime2str(datetime: 'pd.Timestamp | datetime.datetime | Iterable') -> 'list | str':
    if isinstance(datetime, Iterable):
        return list(map(lambda x: x.strftime(r'%Y-%m-%d')))
    else:
        return datetime.strftime(r'%Y-%m-%d')

def str2datetime(string: 'str') -> pd.Timestamp:
    return pd.to_datetime(string)

def logret2algret(logret):
    return np.exp(logret) - 1

def algret2logret(algret):
    return np.log(algret + 1)

def item2list(item) -> list:
    if not isinstance(item, (list, tuple, slice)):
        item = [item]
    return item

def price2ret(open_price: 'pd.DataFrame | pd.Series', close_price: 'pd.DataFrame | pd.Series',
    period: 'Iterable | str' = '1m') -> 'dict':
    open_price = open_price.copy()
    close_price = close_price.copy()
    period = item2list(period)
    ret = {}
    for p in period:
        p = periodkey(p)
        open_price = open_price.resample(p, lable='right').first()
        close_price = close_price.resample(p, lable='right').last()
        ret[p] =  (close_price - open_price) / open_price
    return ret

def price2fwd(open_price: 'pd.DataFrame | pd.Series', close_price: 'pd.DataFrame | pd.Series',
    period: 'Iterable | str' = '1m') -> 'dict':
    open_price = open_price.copy()
    close_price = close_price.copy()
    period = item2list(period)
    ret = {}
    for p in period:
        p = periodkey(p)
        open_price = open_price.resample(p, label='left').first()
        close_price = close_price.resample(p, label='left').last()
        ret[p] = (close_price - open_price) / open_price
    return ret
    
def periodkey(key: 'str') -> 'str':
    match = re.match(r'([ymwdYMWD])(\d+)', key)
    if match:
        return match.group(2) + match.group(1)
    else:
        return key
    