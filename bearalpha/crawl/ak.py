import datetime
import pandas as pd
from ..core import *
from ..tools import *

try:
    import akshare as ak
except:
    pass


class AkShare:
    
    today = pd.to_datetime(datetime.datetime.today()).normalize()
    dbstart = '20050101'
    
    @classmethod
    @Cache(prefix='akshare_market_daily', expire_time=2592000)
    def market_daily(cls, code: str, start: str = None, end: str = None):
        """Get market daily prices for one specific stock
        
        code: str, the code of the stock
        start: str, start date in string format
        end: str, end date in string format
        """
        code = strip_stock_code(code)
        start = start or cls.dbstart
        end = end or time2str(cls.today, formatstr=r'%Y%m%d')

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
        price = ak.stock_zh_a_spot_em()
        price = price.set_index('代码').drop('序号', axis=1)
        if code_only:
            return price.index.to_list()
        return price

    @classmethod
    def plate_quote(cls, name_only: bool = False):
        data = ak.stock_board_industry_name_em()
        data = data.set_index('板块名称')
        if name_only:
            return data.index.to_list()
        return data

    @classmethod
    @Cache(prefix='akshare_etf_market_daily', expire_time=2592000)
    def etf_market_daily(cls, code: str, start: str = None, end: str = None):
        code = strip_stock_code(code)
        start = start or cls.dbstart
        end = end or time2str(cls.today, formatstr=r'%Y%m%d')
        price = ak.fund_etf_fund_info_em(code, start, end).set_index('净值日期')
        price.index = pd.to_datetime(price.index)
        return price
    
    @classmethod
    @Cache(prefix='akshare_stock_fund_flow', expire_time=18000)
    def stock_fund_flow(cls, code: str):
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
    @Cache(prefix='akshare_stock_fund_rank', expire_time=18000)
    def stock_fund_rank(cls):
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
    @Cache(prefix='akshare_plate_info', expire_time=18000)
    def plate_info(cls, plate: str):
        data = ak.stock_board_industry_cons_em(symbol=plate).set_index('代码')
        return data

    @classmethod
    @Cache(prefix='akshare_balance_sheet', expire_time=2592000)
    def balance_sheet(cls, code: str):
        # more infomation, please refer to this website:
        # https://emweb.securities.eastmoney.com/PC_HSF10/NewFinanceAnalysis/Index?type=web&code=sz000001#lrb-0
        code = wrap_stock_code(code, formatstr='{market}{code}', to_lower=True)
        bank = cls.plate_info('银行').index.to_list()
        security = cls.plate_info('证券').index.to_list()
        insurance = cls.plate_info('保险').index.to_list()
        security.pop(security.index('600061'))
        security.pop(security.index('600095'))
        security.pop(security.index('600155'))
        security.pop(security.index('600864'))
        security += ['001236', '000562', '600927', '000987', '002961', '601838', '603093']
        bank += ['600816']
        insurance += ['600291']
        company_type = 3 if code[:2] == "SZ" else 4
        if code[2:] in security:
            company_type = 1
        elif code[2:] in bank:
            company_type = 3
        elif code[2:] in insurance:
            company_type = 2
        url = "https://emweb.securities.eastmoney.com/PC_HSF10/NewFinanceAnalysis/zcfzbDateAjaxNew"
        params = {
            "companyType": company_type,
            "reportDateType": "0",
            "code": code,
        }
        r = Request(url, params=params).get()
        data_json = r.json
        temp_df = pd.DataFrame(data_json["data"])
        temp_df["REPORT_DATE"] = pd.to_datetime(temp_df["REPORT_DATE"]).dt.date
        temp_df["REPORT_DATE"] = temp_df["REPORT_DATE"].astype(str)
        need_date = temp_df["REPORT_DATE"].tolist()
        sep_list = [
            ",".join(need_date[i : i + 5]) for i in range(0, len(need_date), 5)
        ]
        big_df = pd.DataFrame()
        for item in sep_list:
            url = "https://emweb.securities.eastmoney.com/PC_HSF10/NewFinanceAnalysis/zcfzbAjaxNew"
            params = {
                "companyType": company_type,
                "reportDateType": "0",
                "reportType": "1",
                "dates": item,
                "code": code,
            }
            r = Request(url, params=params).get()
            data_json = r.json
            temp_df = pd.DataFrame(data_json["data"])
            big_df = pd.concat([big_df, temp_df], ignore_index=True)
        return big_df
