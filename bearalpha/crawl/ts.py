import datetime
import pandas as pd
import tushare as ts
from ..tools import *
from ..core import *


class TuShare:

    today = time2str(datetime.datetime.today(), formatstr=r'%Y%m%d')
    tsstart = '20050101'
    __api = ts.pro_api(token=Cache().get('tushare'))

    @classmethod
    @Cache(prefix='tushare_stock_basic', expire_time=2592000)
    def stock_basic(cls, code_only=False):
        data = pd.DataFrame()
        while data.empty:
            data = cls.__api.stock_basic(fields='ts_code,symbol,name,area,industry,fullname,'
                'enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
        if code_only:
            return data.ts_code.tolist()
        data.list_date = pd.to_datetime(data.list_date)
        data.delist_date = pd.to_datetime(data.delist_date)
        data = data.set_index('ts_code')
        return data

    @classmethod
    @Cache(prefix='tushare_market_daily', expire_time=2592000)
    def market_daily(cls, code: str, start: str = None, end: str = None):
        start = time2str(start, r'%Y%m%d') or cls.tsstart
        end = time2str(end, r'%Y%m%d') or cls.today
        data = pd.DataFrame()
        while data.empty:
            data = cls.__api.daily(ts_code=code, start_date=start, end_date=end)
        data.trade_date = pd.to_datetime(data.trade_date)
        data = data.set_index('trade_date').drop(['ts_code', 'vol', 'amount'], axis=1)
        data_adj = pd.DataFrame()
        while data_adj.empty:
            data_adj = ts.pro_bar(api=cls.__api, ts_code=code, start_date=start, end_date=end, adj='hfq', adjfactor=True)
        data_adj.trade_date = pd.to_datetime(data_adj.trade_date)
        data_adj = data_adj.set_index('trade_date').drop('ts_code', axis=1).rename(columns=lambda x:
            'adj_' + x if not x.startswith('adj') else x)
        data = pd.concat([data, data_adj], axis=1)
        return data
