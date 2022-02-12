import numpy as np
from utils.io import *
from utils.getdata import *


def last_n_trade_dates(date: Union[datetime.datetime, datetime.date, str],
                       n: int) -> str:
    '''Get last n trading dates
    -------------------------------------
    
    date: str, datetime or date, the given date
    n: int, the number of dates since the given date
    return: list, a list of dates
    '''
    last_date = str2time(date) - datetime.timedelta(days=n * 9 + 1)
    data = pd.read_sql(f'select trade_date from trade_date_daily ' + \
        f'where trade_date >= "{last_date}" and trade_date <= "{date}"',
        stock_database)
    last_date = data.trade_date.sort_values().iloc[-n - 1]
    return time2str(last_date)

def next_n_trade_dates(date: Union[datetime.datetime, datetime.date, str],
                       n: int) -> str:
    '''Get next n trading dates
    -------------------------------------
    
    date: str, datetime or date, the given date
    n: int, the number of dates since the given date
    return: list, a list of dates
    '''
    next_date = str2time(date) + datetime.timedelta(days=n * 9 + 1)
    data = pd.read_sql(f'select trade_date from trade_date_daily ' + \
        f'where trade_date >= "{date}" and trade_date <= "{next_date}"',
        stock_database)
    if len(data) < n + 1:
        return None
    else:
        next_date = data.trade_date.sort_values().iloc[n + 1]
        return time2str(next_date)

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
        forward.name = 'forward_return'
        return forward
    else:
        return np.nan

if __name__ == "__main__":
    print(forward_return('2022-01-10', 5))