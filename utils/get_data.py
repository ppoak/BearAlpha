import datetime
import pandas as pd
from utils.database import *
from typing import Union

def trade_date(start: Union[datetime.date, datetime.datetime, str],
               end: Union[datetime.date, datetime.datetime, str],
               freq: str = 'daily') -> list[datetime.date]:
    '''get trade date
    -------------------------------------

    start: datetime or date or str, start date in 3 forms
    end: datetime or date or str, end date in 3 forms
    freq: str, frequency in either 'daily', 'weekly' or 'monthly'
    '''
    data = pd.read_sql(f'select trade_date from trade_date_{freq} ' + \
        f'where trade_date >= "{start}" ' + \
        f'and trade_date <= "{end}"', stock_database)
    data = data.trade_date.sort_values().tolist()
    return data

def market_daily(start: Union[datetime.date, str, datetime.datetime],
                 end: Union[str, datetime.date, datetime.datetime],
                 fields: list = None,
                 conditions: list = None) -> pd.DataFrame:
    '''get market data in daily frequency
    -------------------------------------

    start: datetime or date or str, start date in 3 forms
    end: datetime or date or str, end date in 3 forms
    fields: list, the field names you want to get
    conditions: list, a series of conditions like "code = '000001.SZ'" listed in a list
    '''
    if fields is None:
        fields = '*'
    else:
        fields = ', '.join(fields)
    sql = f'select {fields} from market_daily where trade_date >= "{start}" and trade_date <= "{end}"'
    if conditions:
        conditions = 'and ' + 'and'.join(conditions)
        sql += conditions
    data = pd.read_sql(sql, stock_database)
    data.trade_date = pd.to_datetime(data.trade_date)
    return data

def index_market_daily(code: str,
                       start: Union[datetime.date, str, datetime.datetime],
                       end: Union[str, datetime.date, datetime.datetime],
                       fields: list = None,
                       conditions: list = None) -> pd.DataFrame:
    '''get index market data in daily frequency
    -------------------------------------

    start: datetime or date or str, start date in 3 forms
    end: datetime or date or str, end date in 3 forms
    code: str, the index code
    fields: list, the field names you want to get
    conditions: list, a series of conditions like "code = '000001.SZ'" listed in a list
    '''
    if fields is None:
        fields = '*'
    else:
        fields = ', '.join(fields)
    sql = f'select {fields} from index_market_daily ' + \
        f'where `s_info_windcode` = "{code}" and ' + \
        f'trade_dt >= "{start}" and trade_dt <= "{end}"'
    if conditions:
        conditions = 'and ' + 'and'.join(conditions)
        sql += conditions
    data = pd.read_sql(sql, stock_database)
    data.trade_dt = pd.to_datetime(data.trade_dt)
    return data

if __name__ == "__main__":
    data = index_market_daily('2021-01-01', '2021-01-05', '000046.SH')
    print(data)
