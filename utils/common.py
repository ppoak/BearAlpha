import numpy as np
from utils.io import *
from utils.getdata import *


def last_n_trade_dates(date: Union[datetime.datetime, datetime.date, str],
                       n: int) -> datetime.date:
    '''Get last n trading dates
    -------------------------------------
    
    date: str, datetime or date, the given date
    n: int, the number of dates since the given date
    return: list, a list of dates
    '''
    last_date = str2time(date) - datetime.timedelta(days=n * 9 + 2)
    data = trade_date(last_date, date)
    last_date = data[-n - 1]
    return last_date

def next_n_trade_dates(date: Union[datetime.datetime, datetime.date, str],
                       n: int) -> datetime.date:
    '''Get next n trading dates
    -------------------------------------
    
    date: str, datetime or date, the given date
    n: int, the number of dates since the given date
    return: list, a list of dates
    '''
    next_date = str2time(date) + datetime.timedelta(days=n * 9 + 2)
    data = trade_date(date, next_date)
    if len(data) < n + 1:
        return None
    else:
        next_date = data[n]
        return next_date

def forward_return(date: Union[datetime.datetime, datetime.date, str],
                   n: int) -> pd.DataFrame:
    '''Get forward return in n trade days
    -------------------------------------
    
    date: str, datetime or date, the given date
    n: int, the number of dates since the given date
    return: list, a list of dates
    '''
    next_date = next_n_trade_dates(date, 1)
    next_n_date = next_n_trade_dates(date, n)
    if next_date is not None and next_n_date is not None:
        next_open = market_daily(next_date, next_date, ['code', 'adjusted_open'])
        next_n_close = market_daily(next_n_date, next_n_date, ['code', 'adjusted_close'])
        forward = (next_n_close.adjusted_close - next_open.adjusted_open) / next_open.adjusted_open
    elif next_date is not None and next_n_date is None:
        next_open = market_daily(next_date, next_date, ['code', 'adjusted_open'])
        next_open['adjusted_open'] = np.nan
        forward = next_open.adjusted_open
    else:
        raise ValueError ('There is no next date')
    forward.name = f'{n}d'
    forward.index = pd.MultiIndex.from_product([[date], forward.index])
    forward.index.names = ["date", "asset"]
    return forward.to_frame()


if __name__ == "__main__":
    print(forward_return('2022-01-23', 10))