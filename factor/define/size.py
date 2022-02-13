import datetime
import pandas as pd
import numpy as np
from typing import Union
from utils.io import *
from utils.common import *
from utils.getdata import *
from utils.treatment import *


def a_c_mkt_cap(date: Union[datetime.datetime, datetime.date, str],
                with_group: bool = True) -> Union[pd.Series, pd.DataFrame]:
    '''log(total circulating A shares * previous close price)
    -----------------------------------------------

    date: str, datetime or date, the given date
    with_group: bool, whether to return the factor with group name
    return: series, a series with the name in accordance with the function name
    '''
    # get stock pool and industry
    stocks = index_hs300_close_weight(date, date, ['s_con_windcode']).s_con_windcode.tolist()
    industry = plate_info(date, date, ['code', 'zxname_level1'])\
        .rename(columns={'zxname_level1': 'group'})
    
    # calculate factor
    free_shares = derivative_indicator(date, date, ['s_info_windcode', 's_dq_mv'])
    factor = np.log(free_shares)

    # factor preprocess
    factor = pd.concat([factor, industry], axis=1)
    factor['s_dq_mv'] = factor.groupby('group').apply(
        lambda x: standard(x.loc[:, 's_dq_mv'])).droplevel(0).sort_index()
    factor['s_dq_mv'] = factor.groupby('group').apply(
        lambda x: deextreme(x.loc[:, 's_dq_mv'], n=3)).droplevel(0).sort_index()
    factor['s_dq_mv'] = missing_fill(factor.s_dq_mv)
    factor = factor.loc[stocks]
    
    # modify factor style
    factor.index = pd.MultiIndex.from_product([[date], factor.index])
    factor.index.names = ["date", "asset"]
    factor = factor.rename(columns={'s_dq_mv': 'a_c_mkt_cap'})
    if with_group:
        return factor
    else:
        factor = factor.loc[:, 'a_c_mkt_cap']
        return factor

def a_mkt_cap(date: Union[datetime.datetime, datetime.date, str],
              with_group: bool = True) -> Union[pd.Series, pd.DataFrame]:
    '''log(total A shares * previous close price)
    -----------------------------------------------

    date: str, datetime or date, the given date
    with_group: bool, whether to return the factor with group name
    return: series, a series with the name in accordance with the function name
    '''
    # get stock pool and industry
    stocks = index_hs300_close_weight(date, date, ['s_con_windcode']).s_con_windcode.tolist()
    industry = plate_info(date, date, ['code', 'zxname_level1'])\
        .rename(columns={'zxname_level1': 'group'})
    
    # calculate factor
    free_shares = derivative_indicator(date, date, ['s_info_windcode', 's_val_mv'])
    factor = np.log(free_shares)

    # factor preprocess
    factor = pd.concat([factor, industry], axis=1)
    factor['s_val_mv'] = factor.groupby('group').apply(
        lambda x: standard(x.loc[:, 's_val_mv'])).droplevel(0).sort_index()
    factor['s_val_mv'] = factor.groupby('group').apply(
        lambda x: deextreme(x.loc[:, 's_val_mv'], n=3)).droplevel(0).sort_index()
    factor['s_val_mv'] = missing_fill(factor.s_val_mv)
    factor = factor.loc[stocks]
    
    # modify factor style
    factor.index = pd.MultiIndex.from_product([[date], factor.index])
    factor.index.names = ["date", "asset"]
    factor = factor.rename(columns={'s_val_mv': 'a_mkt_cap'})
    if with_group:
        return factor
    else:
        factor = factor.loc[:, 'a_c_mkt_cap']
        return factor

def market_cap(date: Union[datetime.datetime, datetime.date, str],
               with_group: bool = True) -> Union[pd.Series, pd.DataFrame]:
    '''log(total shares * previous close price)
    -----------------------------------------------

    date: str, datetime or date, the given date
    with_group: bool, whether to return the factor with group name
    return: series, a series with the name in accordance with the function name
    '''
    # get stock pool and industry
    stocks = index_hs300_close_weight(date, date, ['s_con_windcode']).s_con_windcode.tolist()
    industry = plate_info(date, date, ['code', 'zxname_level1'])\
        .rename(columns={'zxname_level1': 'group'})
        
    # calculate factor
    total_size = derivative_indicator(date, date,
        ['s_info_windcode', 'tot_shr_today', 's_dq_close_today'])
    factor = np.log(total_size.loc[:, 'tot_shr_today'] * total_size.loc[:, 's_dq_close_today'])

    # factor preprocess
    factor.name = 'market_cap'
    factor = pd.concat([factor, industry], axis=1)
    factor['market_cap'] = factor.groupby('group').apply(
        lambda x: standard(x.loc[:, 'market_cap'])).droplevel(0).sort_index()
    factor['market_cap'] = factor.groupby('group').apply(
        lambda x: deextreme(x.loc[:, 'market_cap'], n=3)).droplevel(0).sort_index()
    factor['market_cap'] = missing_fill(factor.market_cap)
    factor = factor.loc[stocks]
    
    # modify factor style
    factor.index = pd.MultiIndex.from_product([[date], factor.index])
    factor.index.names = ["date", "asset"]
    if with_group:
        return factor
    else:
        factor = factor.loc[:, 'market_cap']
        return factor

if __name__ == '__main__':
    print(a_c_mkt_cap('2021-01-05'))