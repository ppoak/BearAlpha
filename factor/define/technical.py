import datetime
import pandas as pd
import numpy as np
from typing import Union
from utils.io import *
from utils.common import *
from utils.getdata import *
from utils.treatment import *

def return_1m(date: Union[datetime.datetime, datetime.date, str]) -> pd.Series:
    '''Stock total return over past 20 trading days
    -----------------------------------------------

    date: str, datetime or date, the given date
    return: series, a series with the name in accordance with the function name
    '''
    # get stock pool and industry
    stocks = index_hs300_close_weight(date, date, ['s_con_windcode']).s_con_windcode.tolist()
    industry = plate_info(date, date, ['code', 'zxname_level1'])
    
    # calculate factor
    last_date = last_n_trade_dates(date, 20)[0]
    market_date = market_daily(date, date, ['code', 'adjusted_close'])
    market_last = market_daily(last_date, last_date, ['code', 'adjusted_close'])    
    factor = (market_date - market_last) / market_last

    # factor preprocess
    factor = pd.concat([factor, industry], axis=1)
    factor['adjusted_close'] = factor.groupby('zxname_level1').apply(
        lambda x: standard(x.loc[:, 'adjusted_close'])).droplevel(0).sort_index()
    factor['adjusted_close'] = factor.groupby('zxname_level1').apply(
        lambda x: deextreme(x.loc[:, 'adjusted_close'], n=3)).droplevel(0).sort_index()
    factor = missing_fill(factor.adjusted_close)
    factor = factor.loc[stocks]
    
    # modify factor style
    factor.name = 'return_1m'
    factor.index = pd.MultiIndex.from_product([[date], factor.index])
    factor.index.names = ["date", "asset"]
    return factor

def return_3m(date: Union[datetime.datetime, datetime.date, str]) -> pd.Series:
    '''Stock total return over past 60 trading days
    -----------------------------------------------

    date: str, datetime or date, the given date
    return: series, a series with the name in accordance with the function name
    '''
    # get stock pool and industry
    stocks = index_hs300_close_weight(date, date, ['s_con_windcode']).s_con_windcode.tolist()
    industry = plate_info(date, date, ['code', 'zxname_level1'])
    
    # calculate factor
    last_date = last_n_trade_dates(date, 60)[0]
    market_date = market_daily(date, date, ['code', 'adjusted_close'])
    market_last = market_daily(last_date, last_date, ['code', 'adjusted_close'])    
    factor = (market_date - market_last) / market_last

    # factor preprocess
    factor = pd.concat([factor, industry], axis=1)
    factor['adjusted_close'] = factor.groupby('zxname_level1').apply(
        lambda x: standard(x.loc[:, 'adjusted_close'])).droplevel(0).sort_index()
    factor['adjusted_close'] = factor.groupby('zxname_level1').apply(
        lambda x: deextreme(x.loc[:, 'adjusted_close'], n=3)).droplevel(0).sort_index()
    factor = missing_fill(factor.adjusted_close)
    factor = factor.loc[stocks]
    
    # modify factor style
    factor.name = 'return_1m'
    factor.index = pd.MultiIndex.from_product([[date], factor.index])
    factor.index.names = ["date", "asset"]
    return factor

def return_12m(date: Union[datetime.datetime, datetime.date, str]) -> pd.Series:
    '''Stock total return over past 250 trading days
    -----------------------------------------------

    date: str, datetime or date, the given date
    return: series, a series with the name in accordance with the function name
    '''
    # get stock pool and industry
    stocks = index_hs300_close_weight(date, date, ['s_con_windcode']).s_con_windcode.tolist()
    industry = plate_info(date, date, ['code', 'zxname_level1'])
    
    # calculate factor
    last_date = last_n_trade_dates(date, 250)[0]
    market_date = market_daily(date, date, ['code', 'adjusted_close'])
    market_last = market_daily(last_date, last_date, ['code', 'adjusted_close'])    
    factor = (market_date - market_last) / market_last

    # factor preprocess
    factor = pd.concat([factor, industry], axis=1)
    factor['adjusted_close'] = factor.groupby('zxname_level1').apply(
        lambda x: standard(x.loc[:, 'adjusted_close'])).droplevel(0).sort_index()
    factor['adjusted_close'] = factor.groupby('zxname_level1').apply(
        lambda x: deextreme(x.loc[:, 'adjusted_close'], n=3)).droplevel(0).sort_index()
    factor = missing_fill(factor.adjusted_close)
    factor = factor.loc[stocks]
    
    # modify factor style
    factor.name = 'return_1m'
    factor.index = pd.MultiIndex.from_product([[date], factor.index])
    factor.index.names = ["date", "asset"]
    return factor


if __name__ == "__main__":
    print(return_12m('2012-01-05'))