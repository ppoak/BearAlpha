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

def turnover_1m(date: Union[datetime.datetime, datetime.date, str]) -> pd.Series:
    '''Stock average turnover in past 20 days, MA(VOLUME/CAPITAL, 20)
    -----------------------------------------------

    date: str, datetime or date, the given date
    return: series, a series with the name in accordance with the function name
    '''
    # get stock pool and industry
    stocks = index_hs300_close_weight(date, date, ['s_con_windcode']).s_con_windcode.tolist()
    industry = plate_info(date, date, ['code', 'zxname_level1'])
    
    # calculate factor
    last_date = last_n_trade_dates(date, 20)[0]
    turnover = derivative_indicator(last_date, date, ['trade_dt', 's_info_windcode', 's_dq_freeturnover'])
    factor = turnover.groupby(level=1).mean()

    # factor preprocess
    factor = pd.concat([factor, industry], axis=1)
    factor['s_dq_freeturnover'] = factor.groupby('zxname_level1').apply(
        lambda x: standard(x.loc[:, 's_dq_freeturnover'])).droplevel(0).sort_index()
    factor['s_dq_freeturnover'] = factor.groupby('zxname_level1').apply(
        lambda x: deextreme(x.loc[:, 's_dq_freeturnover'], n=3)).droplevel(0).sort_index()
    factor = missing_fill(factor.s_dq_freeturnover)
    factor = factor.loc[stocks]
    
    # modify factor style
    factor.name = 'turnover_1m'
    factor.index = pd.MultiIndex.from_product([[date], factor.index])
    factor.index.names = ["date", "asset"]
    return factor

def turnover_3m(date: Union[datetime.datetime, datetime.date, str]) -> pd.Series:
    '''Stock average turnover in past 60 days, MA(VOLUME/CAPITAL, 60)
    -----------------------------------------------

    date: str, datetime or date, the given date
    return: series, a series with the name in accordance with the function name
    '''
    # get stock pool and industry
    stocks = index_hs300_close_weight(date, date, ['s_con_windcode']).s_con_windcode.tolist()
    industry = plate_info(date, date, ['code', 'zxname_level1'])
    
    # calculate factor
    last_date = last_n_trade_dates(date, 60)[0]
    turnover = derivative_indicator(last_date, date, ['trade_dt', 's_info_windcode', 's_dq_freeturnover'])
    factor = turnover.groupby(level=1).mean()

    # factor preprocess
    factor = pd.concat([factor, industry], axis=1)
    factor['s_dq_freeturnover'] = factor.groupby('zxname_level1').apply(
        lambda x: standard(x.loc[:, 's_dq_freeturnover'])).droplevel(0).sort_index()
    factor['s_dq_freeturnover'] = factor.groupby('zxname_level1').apply(
        lambda x: deextreme(x.loc[:, 's_dq_freeturnover'], n=3)).droplevel(0).sort_index()
    factor = missing_fill(factor.s_dq_freeturnover)
    factor = factor.loc[stocks]
    
    # modify factor style
    factor.name = 'turnover_3m'
    factor.index = pd.MultiIndex.from_product([[date], factor.index])
    factor.index.names = ["date", "asset"]
    return factor

def volatility_1m(date: Union[datetime.datetime, datetime.date, str]) -> pd.Series:
    '''Stock total return standard deviation over past 20 trading days
    -----------------------------------------------

    date: str, datetime or date, the given date
    return: series, a series with the name in accordance with the function name
    '''
    # get stock pool and industry
    stocks = index_hs300_close_weight(date, date, ['s_con_windcode']).s_con_windcode.tolist()
    industry = plate_info(date, date, ['code', 'zxname_level1'])
    
    # calculate factor
    last_date = last_n_trade_dates(date, 20)[0]
    market = market_daily(last_date, date, ['trade_date', 'code', 'percent_change'])
    factor = market.groupby(level=1).std()

    # factor preprocess
    factor = pd.concat([factor, industry], axis=1)
    factor['percent_change'] = factor.groupby('zxname_level1').apply(
        lambda x: standard(x.loc[:, 'percent_change'])).droplevel(0).sort_index()
    factor['percent_change'] = factor.groupby('zxname_level1').apply(
        lambda x: deextreme(x.loc[:, 'percent_change'], n=3)).droplevel(0).sort_index()
    factor = missing_fill(factor.percent_change)
    factor = factor.loc[stocks]
    
    # modify factor style
    factor.name = 'volatility_1m'
    factor.index = pd.MultiIndex.from_product([[date], factor.index])
    factor.index.names = ["date", "asset"]
    return factor

def volatility_3m(date: Union[datetime.datetime, datetime.date, str]) -> pd.Series:
    '''Stock total return standard deviation over past 20 trading days
    -----------------------------------------------

    date: str, datetime or date, the given date
    return: series, a series with the name in accordance with the function name
    '''
    # get stock pool and industry
    stocks = index_hs300_close_weight(date, date, ['s_con_windcode']).s_con_windcode.tolist()
    industry = plate_info(date, date, ['code', 'zxname_level1'])
    
    # calculate factor
    last_date = last_n_trade_dates(date, 60)[0]
    market = market_daily(last_date, date, ['trade_date', 'code', 'percent_change'])
    factor = market.groupby(level=1).std()

    # factor preprocess
    factor = pd.concat([factor, industry], axis=1)
    factor['percent_change'] = factor.groupby('zxname_level1').apply(
        lambda x: standard(x.loc[:, 'percent_change'])).droplevel(0).sort_index()
    factor['percent_change'] = factor.groupby('zxname_level1').apply(
        lambda x: deextreme(x.loc[:, 'percent_change'], n=3)).droplevel(0).sort_index()
    factor = missing_fill(factor.percent_change)
    factor = factor.loc[stocks]
    
    # modify factor style
    factor.name = 'volatility_1m'
    factor.index = pd.MultiIndex.from_product([[date], factor.index])
    factor.index.names = ["date", "asset"]
    return factor

def volatility_12m(date: Union[datetime.datetime, datetime.date, str]) -> pd.Series:
    '''Stock total return standard deviation over past 20 trading days
    -----------------------------------------------

    date: str, datetime or date, the given date
    return: series, a series with the name in accordance with the function name
    '''
    # get stock pool and industry
    stocks = index_hs300_close_weight(date, date, ['s_con_windcode']).s_con_windcode.tolist()
    industry = plate_info(date, date, ['code', 'zxname_level1'])
    
    # calculate factor
    last_date = last_n_trade_dates(date, 250)[0]
    market = market_daily(last_date, date, ['trade_date', 'code', 'percent_change'])
    factor = market.groupby(level=1).std()

    # factor preprocess
    factor = pd.concat([factor, industry], axis=1)
    factor['percent_change'] = factor.groupby('zxname_level1').apply(
        lambda x: standard(x.loc[:, 'percent_change'])).droplevel(0).sort_index()
    factor['percent_change'] = factor.groupby('zxname_level1').apply(
        lambda x: deextreme(x.loc[:, 'percent_change'], n=3)).droplevel(0).sort_index()
    factor = missing_fill(factor.percent_change)
    factor = factor.loc[stocks]
    
    # modify factor style
    factor.name = 'volatility_1m'
    factor.index = pd.MultiIndex.from_product([[date], factor.index])
    factor.index.names = ["date", "asset"]
    return factor

def ar(date: Union[datetime.datetime, datetime.date, str]) -> pd.Series:
    '''Sum of (daily high price - daily open price)/(daily open price - daily low price) of previous 20 days
    -----------------------------------------------

    date: str, datetime or date, the given date
    return: series, a series with the name in accordance with the function name
    '''
    # get stock pool and industry
    stocks = index_hs300_close_weight(date, date, ['s_con_windcode']).s_con_windcode.tolist()
    industry = plate_info(date, date, ['code', 'zxname_level1'])
    
    # calculate factor
    last_date = last_n_trade_dates(date, 20)[0]
    market = market_daily(last_date, date, 
        ['trade_date', 'code', 'adjusted_high', 'adjusted_low', 'adjusted_open'])
    factor = market.groupby(level=1).apply(lambda x:
        ((x.adjusted_high - x.adjusted_open) / (x.adjusted_open - x.adjusted_low)).sum())
    factor = factor.replace(np.inf, np.nan)

    # factor preprocess
    factor.name = 'ar'
    factor = pd.concat([factor, industry], axis=1)
    factor['ar'] = factor.groupby('zxname_level1').apply(
        lambda x: standard(x.loc[:, 'ar'])).droplevel(0).sort_index()
    factor['ar'] = factor.groupby('zxname_level1').apply(
        lambda x: deextreme(x.loc[:, 'ar'], n=3)).droplevel(0).sort_index()
    factor = missing_fill(factor.ar)
    factor = factor.loc[stocks]
    
    # modify factor style
    factor.index = pd.MultiIndex.from_product([[date], factor.index])
    factor.index.names = ["date", "asset"]
    return factor

def br(date: Union[datetime.datetime, datetime.date, str]) -> pd.Series:
    '''sum of maximum(0, (high - previous close price)) / sum of maximum(0, (previous close price - low)) of previous 20 days
    -----------------------------------------------

    date: str, datetime or date, the given date
    return: series, a series with the name in accordance with the function name
    '''
    # get stock pool and industry
    stocks = index_hs300_close_weight(date, date, ['s_con_windcode']).s_con_windcode.tolist()
    industry = plate_info(date, date, ['code', 'zxname_level1'])
    
    # calculate factor
    last_date = last_n_trade_dates(date, 20)[0]
    market = market_daily(last_date, date, 
        ['trade_date', 'code', 'adjusted_preclose', 'adjusted_high', 'adjusted_low'])
    factor = market.groupby(level=1).apply(lambda x:
        (x.loc[:, 'adjusted_high'] - x.loc[:, 'adjusted_preclose']).clip(0).sum() / 
        (x.loc[:, 'adjusted_preclose'] - x.loc[:, 'adjusted_low']).clip(0).sum())

    # factor preprocess
    factor.name = 'br'
    factor = pd.concat([factor, industry], axis=1)
    factor['br'] = factor.groupby('zxname_level1').apply(
        lambda x: standard(x.loc[:, 'br'])).droplevel(0).sort_index()
    factor['br'] = factor.groupby('zxname_level1').apply(
        lambda x: deextreme(x.loc[:, 'br'], n=3)).droplevel(0).sort_index()
    factor = missing_fill(factor.br)
    factor = factor.loc[stocks]
    
    # modify factor style
    factor.index = pd.MultiIndex.from_product([[date], factor.index])
    factor.index.names = ["date", "asset"]
    return factor

def bias_1m(date: Union[datetime.datetime, datetime.date, str]) -> pd.Series:
    '''(last close price - 20 days close price moving average) / 20 days close price moving average
    -----------------------------------------------

    date: str, datetime or date, the given date
    return: series, a series with the name in accordance with the function name
    '''
    # get stock pool and industry
    stocks = index_hs300_close_weight(date, date, ['s_con_windcode']).s_con_windcode.tolist()
    industry = plate_info(date, date, ['code', 'zxname_level1'])
    
    # calculate factor
    last_date = last_n_trade_dates(date, 20)[0]
    market = market_daily(last_date, date, 
        ['trade_date', 'code', 'adjusted_close'])
    factor = market.groupby(level=1).apply(lambda x:
        ((x.loc[date, 'adjusted_close'] - x.loc[:, 'adjusted_close'].mean()) / 
        x.loc[:, 'adjusted_close'].mean()).values[0])

    # factor preprocess
    factor.name = 'bias_1m'
    factor = pd.concat([factor, industry], axis=1)
    factor['bias_1m'] = factor.groupby('zxname_level1').apply(
        lambda x: standard(x.loc[:, 'bias_1m'])).droplevel(0).sort_index()
    factor['bias_1m'] = factor.groupby('zxname_level1').apply(
        lambda x: deextreme(x.loc[:, 'bias_1m'], n=3)).droplevel(0).sort_index()
    factor = missing_fill(factor.bias_1m)
    factor = factor.loc[stocks]
    
    # modify factor style
    factor.index = pd.MultiIndex.from_product([[date], factor.index])
    factor.index.names = ["date", "asset"]
    return factor

def davol_1m(date: Union[datetime.datetime, datetime.date, str]) -> pd.Series:
    '''Stock Turnover 20 days / Turnover 120 days
    -----------------------------------------------

    date: str, datetime or date, the given date
    return: series, a series with the name in accordance with the function name
    '''
    def _cal(x):
        s = x.loc[last_short_date:date, 's_dq_freeturnover'].mean()
        l = x.loc[last_long_date:date, 's_dq_freeturnover'].mean()
        if l == 0:
            return np.nan
        return s / l

    # get stock pool and industry
    stocks = index_hs300_close_weight(date, date, ['s_con_windcode']).s_con_windcode.tolist()
    industry = plate_info(date, date, ['code', 'zxname_level1'])
    
    # calculate factor
    last_long_date = last_n_trade_dates(date, 120)[0]
    last_short_date = last_n_trade_dates(date, 20)[0]
    date = str2time(date)
    last_long_date = str2time(last_long_date)
    last_short_date = str2time(last_short_date)
    market = derivative_indicator(last_long_date, date, 
        ['trade_dt', 's_info_windcode', 's_dq_freeturnover'])
    factor = market.groupby(level=1).apply(_cal)

    # factor preprocess
    factor.name = 'davol_1m'
    factor = pd.concat([factor, industry], axis=1)
    factor['davol_1m'] = factor.groupby('zxname_level1').apply(
        lambda x: standard(x.loc[:, 'davol_1m'])).droplevel(0).sort_index()
    factor['davol_1m'] = factor.groupby('zxname_level1').apply(
        lambda x: deextreme(x.loc[:, 'davol_1m'], n=3)).droplevel(0).sort_index()
    factor = missing_fill(factor.davol_1m)
    factor = factor.loc[stocks]
    
    # modify factor style
    factor.index = pd.MultiIndex.from_product([[date], factor.index])
    factor.index.names = ["date", "asset"]
    return factor


if __name__ == "__main__":
    print(davol_1m('2012-01-05'))