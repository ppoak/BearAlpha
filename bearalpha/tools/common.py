"""Some common functions to be used in quool,
like convert some type of data into another type of data,
or some common tools to get basic trading date infomation

@author: ppoak
@date: 2022-04-27
@email: oakery@qq.com
"""

import re
import datetime
import pandas as pd


MICROSECOND = datetime.timedelta(microseconds=1)
SECOND = datetime.timedelta(seconds=1)
MINUTE = datetime.timedelta(minutes=1)
HOUR = datetime.timedelta(hours=1)
DAY = datetime.timedelta(days=1)
WEEK = datetime.timedelta(days=7)
MONTH = datetime.timedelta(days=30)
YEAR = datetime.timedelta(days=365)

def time2str(date: 'str | datetime.datetime | int | datetime.date') -> str:
    """convert a datetime class to time-like string"""
    if isinstance(date, int):
        date = str(date)
    date = pd.to_datetime(date)
    date = date.strftime(r'%Y-%m-%d')
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


if __name__ == '__main__':
    pass