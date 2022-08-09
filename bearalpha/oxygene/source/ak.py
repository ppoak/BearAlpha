import datetime
import pandas as pd
from ..base import *
from ...tools import *


class AkShare:
    
    today = pd.to_datetime(datetime.datetime.today()).normalize()
    dbstart = '20050101'
    
    @classmethod
    @cache(prefix='akshare_market_daily', expire=2592000)
    def market_daily(cls, code: str, start: str = None, end: str = None):
        """Get market daily prices for one specific stock
        
        code: str, the code of the stock
        start: str, start date in string format
        end: str, end date in string format
        """
        import akshare as ak
        code = strip_stock_code(code)
        start = time2str(start, formatstr=r'%Y%m%d') or cls.dbstart
        end = time2str(end, formatstr=r'%Y%m%d') or time2str(cls.today, formatstr=r'%Y%m%d')

        price = ak.stock_zh_a_hist(symbol=code, start_date=start, end_date=end, adjust='')
        if not price.empty:
            price = price.set_index('日期')
        else:
            return price
        adjprice = ak.stock_zh_a_hist(symbol=code, start_date=start, end_date=end, adjust='hfq')
        if not adjprice.empty:
            adjprice = adjprice.set_index('日期')
        else:
            return adjprice
        adjprice = adjprice.drop(["成交量", "成交额", "换手率"], axis=1).rename(columns=lambda x: f"复权{x}")
        price = pd.concat([price, adjprice], axis=1)
        price.index = pd.to_datetime(price.index)

        return price

    @classmethod
    def stock_quote(cls, code_only: bool = False):
        """Get the realtime quote amoung the a stock share market

        code_only: bool, decide only return codes on the market
        """
        import akshare as ak
        price = ak.stock_zh_a_spot_em()
        price = price.set_index('代码').drop('序号', axis=1)
        if code_only:
            return price.index.to_list()
        return price

    @classmethod
    def plate_quote(cls, name_only: bool = False):
        import akshare as ak
        data = ak.stock_board_industry_name_em()
        data = data.set_index('板块名称')
        if name_only:
            return data.index.to_list()
        return data

    @classmethod
    @cache(prefix='akshare_etf_market_daily', expire=2592000)
    def etf_market_daily(cls, code: str, start: str = None, end: str = None):
        import akshare as ak
        code = strip_stock_code(code)
        start = start or cls.dbstart
        end = end or time2str(cls.today, formatstr=r'%Y%m%d')
        price = ak.fund_etf_fund_info_em(code, start, end).set_index('净值日期')
        price.index = pd.to_datetime(price.index)
        return price
    
    @classmethod
    @cache(prefix='akshare_stock_fund_flow', expire=18000)
    def stock_fund_flow(cls, code: str):
        import akshare as ak
        code, market = code.split('.')
        if market.isdigit():
            code, market = market, code
        market = market.lower()
        funds = ak.stock_individual_fund_flow(stock=code, market=market)
        funds = funds.set_index('日期')
        funds.index = pd.MultiIndex.from_product([[code], 
            pd.to_datetime(funds.index)], names=['日期', '代码'])
        return funds
    
    @classmethod
    @cache(prefix='akshare_stock_fund_rank', expire=18000)
    def stock_fund_rank(cls):
        import akshare as ak
        datas = []
        for indi in ['今日', '3日', '5日', '10日']:
            datas.append(ak.stock_individual_fund_flow_rank(indicator=indi
                ).drop('序号', axis=1).set_index('代码').rename(columns={'最新价': f'{indi}最新价'}))
        datas = pd.concat(datas, axis=1)
        datas['简称'] = datas.iloc[:, 0]
        datas = datas.drop('名称', axis=1)
        datas = datas.replace('-', None).apply(pd.to_numeric, errors='ignore')
        datas.index = pd.MultiIndex.from_product([[cls.today], datas.index], names=['日期', '代码'])
        return datas
    
    @classmethod
    @cache(prefix='akshare_plate_info', expire=18000)
    def plate_info(cls, plate: str):
        import akshare as ak
        data = ak.stock_board_industry_cons_em(symbol=plate).set_index('代码')
        return data

    @classmethod
    @cache(prefix='akshare_balance_sheet', expire=2592000)
    def balance_sheet(cls, code: str):
        # more infomation, please refer to this website:
        # https://emweb.securities.eastmoney.com/PC_HSF10/NewFinanceAnalysis/Index?type=web&code=sz000001#lrb-0
        import akshare as ak
        code = wrap_stock_code(code, formatstr='{market}{code}')
        data = ak.stock_balance_sheet_by_report_em(symbol=code)
        if data.empty:
            return data
        else:
            data.REPORT_DATE = pd.to_datetime(data.REPORT_DATE)
            data = data.set_index('REPORT_DATE')
            return data