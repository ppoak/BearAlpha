import re
import datetime
import pandas as pd
from ..core import *
from ..tools import *

try:
    import akshare as ak
except:
    pass


class AkShare:
    
    today = datetime.datetime.today()
    dbstart = '20050101'

    @staticmethod
    def strip_code(code: str):
        code_pattern = r'\.?[Ss][Zz]\.?|\.?[Ss][Hh]\.?|\.?[Bb][Jj]\.?'\
            '|\.?[Oo][Ff]\.?'
        return re.sub(code_pattern, '', code)
    
    @classmethod
    @Cache(prefix='akshare_market_daily', expire_time=2592000)
    def market_daily(cls, code: str, start: str = None, end: str = None):
        """Get market daily prices for one specific stock
        
        code: str, the code of the stock
        start: str, start date in string format
        end: str, end date in string format
        """
        code = cls.strip_code(code)
        start = start or cls.dbstart
        end = end or time2str(cls.today, formatstr=r'%Y%m%d')

        price = ak.stock_zh_a_hist(symbol=code, start_date=start, end_date=end, adjust='').set_index('日期')
        adjprice = ak.stock_zh_a_hist(symbol=code, start_date=start, end_date=end, adjust='hfq').set_index('日期')
        adjprice = adjprice.drop(["成交量", "成交额", "换手率"], axis=1).rename(columns=lambda x: f"复权{x}")
        price = pd.concat([price, adjprice], axis=1)
        price.index = pd.to_datetime(price.index)

        return price

    @classmethod
    def stock_quote(cls, code_only: bool = False):
        """Get the realtime quote amoung the a stock share market

        code_only: bool, decide only return codes on the market
        """
        price = ak.stock_zh_a_spot_em()
        price = price.set_index('代码').drop('序号', axis=1)
        if code_only:
            return price.index.to_list()
        return price


    @classmethod
    def etf_market_daily(cls, code: str, start: str = None, end: str = None):
        code = cls.strip_code(code)
        start = start or cls.dbstart
        end = end or time2str(cls.today, formatstr=r'%Y%m%d')
        price = ak.fund_etf_fund_info_em(code, start, end).set_index('净值日期')
        price.index = pd.to_datetime(price.index)
        return price
        