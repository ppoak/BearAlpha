import re
import pandas as pd
import numpy as np
from utils import *
from typing import Union
from factor.utils.config import FACTORS

def factor_datas(factors: Union[list, str], dates: Union[list, str]) -> pd.DataFrame():
    '''Get factor data
    --------------------------------

    factors: str or list, factor names can be provided in list or str
    dates: str or list, date can be provided in list or str
    return: pd.DataFrame, a dataframe with multi-index and columns
        representing factor value
    '''
    factors = item2list(factors)
    dates = item2list(dates)

    datas = []
    for factor in factors:
        data = pd.DataFrame()
        factor_loader = FACTORS[factor]["dataloader"]
        for date in dates:
            data = data.append(factor_loader(date))
        datas.append(data)
    
    datas = pd.concat(datas, axis=1)
    datas = datas.sort_index(level=[0, 1])
    group = datas.loc[:, 'group'].iloc[:, 0]
    datas = datas.drop('group', axis=1)
    datas['group'] = group
    return datas

def forward_returns(dates: Union[list, str], n: Union[list, int]) -> pd.DataFrame():
    '''Get forward return
    --------------------------------

    dates: str or list, date can be provided in list or str
    n: str or list, forward period can be provided in list or int
    return: pd.DataFrame, a dataframe with multi-index and columns
        representing factor value
    '''
    dates = item2list(dates)
    n = item2list(n)

    datas = []
    for p in n:
        data = pd.DataFrame()
        for date in dates:
            data = data.append(forward_return(date, p))
        datas.append(data)

    datas = pd.concat(datas, axis=1)
    datas = datas.sort_index(level=[0, 1])
    return datas

def factor_datas_and_forward_returns(factors: Union[list, str],
                                     dates: Union[list, str],
                                     forward_period: Union[list, int] = 20) -> pd.DataFrame():
    '''A combination of factor_datas and forward_returns
    ---------------------------------------------------

    factors: str or list, factor names can be provided in list or str
    dates: str or list, date can be provided in list or str
    forward_period: str or list, forward period can be provided in list or int
    return: pd.DataFrame, a dataframe with multi-index and columns
    '''
    factor = factor_datas(factors, dates)
    forward = forward_returns(dates, forward_period)
    datas = pd.merge(factor, forward, left_index=True, right_index=True)
    return datas

def get_factor_columns(data: pd.DataFrame):
    matched = list(filter(lambda x: 
        x != 'group' and not re.match(r'\d+d', x),
        data.columns))
    return matched
    
def get_forward_return_columns(data: pd.DataFrame):
    matched = list(filter(lambda x: re.match(r'\d+[dmy]', x), data.columns))
    return matched


if __name__ == "__main__":
    factors = ['return_1m', 'return_3m']
    dates = trade_date('2011-02-01', '2011-12-31', freq='monthly')
    forward_period = [1, 3]
    data = factor_datas_and_forward_returns(factors, dates, forward_period)
    print(data)
    forward_columns = get_forward_return_columns(data)
    print(forward_columns)
    factor_columns = get_factor_columns(data)
    print(factor_columns)
    