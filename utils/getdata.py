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
    # get data
    if fields is None:
        fields = '*'
    else:
        fields = ', '.join(fields)
    sql = f'select {fields} from market_daily where trade_date >= "{start}" and trade_date <= "{end}"'
    if conditions:
        conditions = 'and ' + 'and'.join(conditions)
        sql += conditions
    data = pd.read_sql(sql, stock_database)

    # modify time format
    if 'trade_date' in fields:
        data.trade_date = pd.to_datetime(data.trade_date)

    # modify dataframe index
    index = ['trade_date', 'code']
    index = list(filter(lambda x: x in fields, index))
    if index:
        data = data.set_index(index)
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
    # get data
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

    # modify time format
    if 'trade_dt' in fields:
        data.trade_dt = pd.to_datetime(data.trade_dt)

    # modify dataframe index
    index = ['trade_dt', 's_info_windcode']
    index = list(filter(lambda x: x in fields, index))
    if index:
        data = data.set_index(index)
    return data

def index_hs300_close_weight(start: Union[datetime.datetime, datetime.date, str],
                              end: Union[datetime.datetime, datetime.date, str],
                              fields: list = None,
                              conditions: list = None) -> pd.DataFrame:
    '''get index hs300 close weight in daily frequency
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
    sql = f'select {fields} from index_hs300_close_weight ' + \
        f'where trade_dt >= "{start}" and trade_dt <= "{end}"'
    if conditions:
        conditions = 'and ' + 'and'.join(conditions)
        sql += conditions
    data = pd.read_sql(sql, stock_database)
    if data.get('trade_date', False):
        data.trade_dt = pd.to_datetime(data.trade_dt)
    return data

def plate_info(start: Union[datetime.datetime, datetime.date, str],
               end: Union[datetime.datetime, datetime.date, str],
               fields: list = None,
               conditions: list = None) -> pd.DataFrame:
    '''get plate info in daily frequency
    -------------------------------------

    start: datetime or date or str, start date in 3 forms
    end: datetime or date or str, end date in 3 forms
    fields: list, the field names you want to get
    conditions: list, a series of conditions like "code = '000001.SZ'" listed in a list
    '''
    # get data
    if fields is None:
        fields = '*'
    else:
        fields = ', '.join(fields)
    sql = f'select {fields} from plate_info ' + \
        f'where trade_date >= "{start}" and trade_date <= "{end}"'
    if conditions:
        conditions = 'and ' + 'and'.join(conditions)
        sql += conditions
    data = pd.read_sql(sql, stock_database)

    # modify time format
    if 'trade_date' in fields:
        data.trade_date = pd.to_datetime(data.trade_date)
    
    # modify dataframe index
    index = ['trade_date', 'code']
    index = list(filter(lambda x: x in fields, index))
    if index:
        data = data.set_index(index)
    return data

if __name__ == "__main__":
    data = plate_info('2021-01-05', '2021-01-05', ['code', 'swcode_level1'])
    print(data)
