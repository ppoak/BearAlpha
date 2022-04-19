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
    
def category2dummy(data: 'pd.DataFrame | pd.Series', **kwargs) -> pd.DataFrame:
    dummydata = pd.get_dummies(data, **kwargs)
    return dummydata

def dummy2category(data: 'pd.DataFrame | pd.Series', category_name: str = None) -> pd.DataFrame:
    columns = pd.DataFrame(
        data.columns.values.reshape((1, -1)).repeat(data.shape[0], axis=0),
        index=data.index, columns=data.columns)
    category = columns[data.astype('bool')].replace(np.nan, '').astype('str').sum(axis=1)
    if category_name:
        category = category.to_frame(name=category_name)
    return category

if __name__ == "__main__":
    a = pd.read_csv("assets/data/stock.nosync/status/df_stock_status_2005-01-04.csv", index_col=0, parse_dates=True)
    a = a.iloc[:, list([2]) + list(range(22, 52))]
    a = a.set_index('stock_id')
    print(dummy2category(a, 'group'))