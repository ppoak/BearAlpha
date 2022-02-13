import pandas as pd
from utils import *
from typing import Union
from factor.utils.config import FACTORS

def factor_datas(factors: Union[list, str], dates: Union[list, str]) -> pd.DataFrame():
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
    datas = datas.T.drop_duplicates(keep='last').T
    return datas

def forward_returns(dates: Union[list, str], n: Union[list, int]) -> pd.DataFrame():
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
    factor = factor_datas(factors, dates)
    forward = forward_returns(dates, forward_period)
    datas = pd.merge(factor, forward, left_index=True, right_index=True)
    return datas

if __name__ == "__main__":
    factors = ['return_1m', 'return_3m']
    dates = trade_date('2011-02-01', '2021-12-31', freq='monthly')
    forward_period = [1, 3, 5, 20]
    print(factor_datas_and_forward_returns(factors, dates, forward_period))
    