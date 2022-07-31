"""Some common functions to be used in quool
============================================

Like convert some type of data into another type of data,
or some common tools to get basic trading date infomation

Examples:
-----------

>>> import bearalpha as ba
>>> ba.last_report_period('2015-01-01')
"""

import re
import time
import datetime
import pandas as pd
from functools import wraps


def time2str(date: 'str | datetime.datetime | int | datetime.date', formatstr: str = r'%Y-%m-%d') -> str:
    """convert a datetime class to time-like string"""
    if isinstance(date, int):
        date = str(date)
    date = pd.to_datetime(date)
    if isinstance(date, datetime.datetime):
        date = date.strftime(formatstr)
    return date

def str2time(date: 'str | datetime.datetime') -> datetime.datetime:
    """convert a time-like string to datetime class"""
    if isinstance(date, (str, datetime.date)):
        date = pd.to_datetime(date)
    elif isinstance(date, (float, int)):
        date = pd.to_datetime(str(int(date)))
    return date

def item2list(item) -> list:
    """convert a non list item to a list"""
    if item is None:
        return []
    elif not isinstance(item, list):
        return [item]
    else:
        return item

def item2tuple(item) -> list:
    """convert a non tuple item to a tuple"""
    if not isinstance(item, tuple):
        return (item, )
    else:
        return item
        
def hump2snake(hump: str) -> str:
    """convert hump name to snake name"""
    snake = re.sub(r'([a-z]|\d)([A-Z])', r'\1_\2', hump).lower()
    return snake

def strip_stock_code(code: str):
    code_pattern = r'\.?[Ss][Zz]\.?|\.?[Ss][Hh]\.?|\.?[Bb][Jj]\.?'\
        '|\.?[Oo][Ff]\.?'
    return re.sub(code_pattern, '', code)
    
def wrap_stock_code(code: str, formatstr: str = '{code}.{market}', to_lower: bool = False):
    if len(code.split('.')) != 1:
        raise ValueError('It seems your code is already wrapped')
    sh_code_pat = r'6\d{5}|9\d{5}'
    sz_code_pat = r'0\d{5}|2\d{5}|3\d{5}'
    bj_code_pat = r'4\d{5}|8\d{5}'
    if re.match(sh_code_pat, code):
        return formatstr.format(code=code, market='sh' if to_lower else 'SH')
    elif re.match(sz_code_pat, code):
        return formatstr.format(code=code, market='sz' if to_lower else 'SZ')
    elif re.match(bj_code_pat, code):
        return formatstr.format(code=code, market='bj' if to_lower else 'BJ')
    else:
        raise ValueError('No pattern can match your code, please check it')

def latest_report_period(date: 'str | datetime.datetime | datetime.date',
    n: int = 1) -> 'str | list[str]':
    """Get the nearest n report period
    ----------------------------------

    date: str, datetime or date, the given date
    n: int, the number of report periods before the given date,
    """
    date = str2time(date)
    this_year = date.year
    last_year = this_year - 1
    nearest_report_date = {
        "01-01": str(last_year) + "-09-30",
        "04-30": str(this_year) + "-03-31",
        "08-31": str(this_year) + "-06-30",
        "10-31": str(this_year) + "-09-30"
    }
    report_date = list(filter(lambda x: x <= date.strftime(r'%Y-%m-%d')[-5:], 
        nearest_report_date.keys()))[-1]
    report_date = nearest_report_date[report_date]
    fundmental_dates = pd.date_range(end=report_date, periods=n, freq='q')
    fundmental_dates = list(map(lambda x: x.strftime(r'%Y-%m-%d'), fundmental_dates))
    return fundmental_dates

def timeit(func):
    wraps(func)
    def decorated(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        spent = end - start

        if spent <= 60:
            timestr = f'{spent: .4f}s'
        
        elif spent <= 3600:
            minute = spent // 60
            second = spent - minute * 60
            timestr = f'{minute}m {second: .4f}s'
            
        else:
            hour = spent // 3600
            minute = (spent - hour * 3600) // 60
            second = spent - hour * 3600 - minute * 60
            timestr = f'{hour}h {minute}m {second:.4f}s'
        
        print(f'{func.__name__} Time Spent: {timestr}')
        return result
    return decorated


if __name__ == '__main__':
    pass