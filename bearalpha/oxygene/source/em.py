import datetime
import pandas as pd
from ..core import *
from ..tools import *


class Em:
    __data_center = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    @classmethod
    @Cache(prefix='Em_active_opdep', expire_time=2592000)
    def active_opdep(cls, date: 'str | datetime.datetime') -> pd.DataFrame:
        '''Update data for active oprate department
        --------------------------------------------

        date: str or datetime, the given date
        return: pd.DataFrame, a dataframe containing information on eastmoney
        '''
        date = time2str(date)
        params = {
            "sortColumns": "TOTAL_NETAMT,ONLIST_DATE,OPERATEDEPT_CODE",
            "sortTypes": "-1,-1,1",
            "pageSize": 100000,
            "pageNumber": 1,
            "reportName": "RPT_OPERATEDEPT_ACTIVE",
            "columns": "ALL",
            "source": "WEB",
            "client": "WEB",
            "filter": f"(ONLIST_DATE>='{date}')(ONLIST_DATE<='{date}')"
        }
        headers = {
            "Referer": "https://data.eastmoney.com/"
        }
        res = Request(cls.__data_center, headers=headers, params=params).get().json
        data = res['result']['data']
        data = pd.DataFrame(data)
        data = data.rename(columns=dict(zip(data.columns, data.columns.map(lambda x: x.lower()))))
        data.onlist_date = pd.to_datetime(data.onlist_date)
        return data
    
    @classmethod
    @Cache(prefix='Em_active_opdep_details', expire_time=2592000)
    def active_opdep_details(cls, date: 'str | datetime.datetime') -> pd.DataFrame:
        date = time2str(date)
        params = {
            "sortColumns": "TOTAL_NETAMT,ONLIST_DATE,OPERATEDEPT_CODE",
            "sortTypes": "-1,-1,1",
            "pageSize": 100000,
            "pageNumber": 1,
            "reportName": "RPT_OPERATEDEPT_ACTIVE",
            "columns": "ALL",
            "source": "WEB",
            "client": "WEB",
            "filter": f"(ONLIST_DATE>='{date}')(ONLIST_DATE<='{date}')"
        }
        headers = {
            "Referer": "https://data.eastmoney.com/"
        }
        res = Request(cls.__data_center, headers=headers, params=params).get().json
        data = res['result']['data']
        data = pd.DataFrame(data)
        data = data.rename(columns=dict(zip(data.columns, data.columns.map(lambda x: x.lower()))))
        data.onlist_date = pd.to_datetime(data.onlist_date)
        data_parent = data[['onlist_date', 'operatedept_code']]
        datas = []
        for i in range(len(data)):
            opdep_code = data_parent.iloc[i, :]['operatedept_code']
            params = {
                "sortColumns": "TRADE_DATE,SECURITY_CODE",
                "sortTypes": "-1,1",
                "pageSize": 100000,
                "pageNumber": 1,
                "reportName": "RPT_OPERATEDEPT_TRADE_DETAILS",
                "columns": "ALL",
                "filter": f"(OPERATEDEPT_CODE={opdep_code})",
                "source": "WEB",
                "client": "WEB"
            }
            headers = {
                "Referer": "https://data.eastmoney.com/"
            }
            res = Request(cls.__data_center, headers=headers, params=params).get().json
            data = res['result']['data']
            data = pd.DataFrame(data)
            datas.append(data)
            
        datas = pd.concat(datas, axis=0).reset_index(drop=True)
        datas = datas.rename(columns=dict(zip(datas.columns, datas.columns.map(lambda x: x.lower()))))
        datas.trade_date = pd.to_datetime(datas.trade_date)
        datas = datas.loc[datas['trade_date'] == date]
        return datas
        
    @classmethod
    @Cache(prefix='Em_institution_trade', expire_time=2592000)
    def institution_trade(cls, date: 'str | datetime.datetime') -> pd.DataFrame:
        date = time2str(date)
        params = {
            'sortColumns' : 'NET_BUY_AMT,TRADE_DATE,SECURITY_CODE',
            'sortTypes' : '-1,-1,1',
            'pageSize' : '100000',
            'pageNumber' : '1',
            'reportName' : 'RPT_ORGANIZATION_TRADE_DETAILS',
            'columns' : 'ALL', 
            'source' : 'WEB',
            'client' :  'WEB', 
            'filter' : f"(TRADE_DATE='{date}')"
        }
    
        headers = {
            "Referer": "https://data.eastmoney.com/"
        }
        res = Request(cls.__data_center, headers=headers, params=params).get().json
        data = res['result']['data']
        data = pd.DataFrame(data)
        data = data.rename(columns=dict(zip(data.columns, data.columns.map(lambda x: x.lower()))))
        data.trade_date = pd.to_datetime(data.trade_date)
        return data
        
    @classmethod
    @Cache(prefix='Em_oversea_institution_holding', expire_time=2592000)
    def oversea_institution_holding(cls, date: 'str | datetime.datetime') -> pd.DataFrame:
        import requests
        import numpy as np
        import re
        date = time2str(date)
        main_page = 'https://data.eastmoney.com/hsgtcg/InstitutionQueryMore.html'
        res = requests.get(main_page)
        res.raise_for_status()
        institution_list = re.findall(r'var jgList= \[.*\];', res.text)[0].split('=')[1].strip(';')
        institution_list = eval(institution_list)
        datas = []
        for institution in institution_list:
            name = institution['PARTICIPANT_CODE']
            callbackfunc = 'jQuery1123032132491687413733_1646408202496'
            for i in range(1, 10):
                params = {
                    'callback': callbackfunc,
                    'sortColumns': 'HOLD_DATE',
                    'sortTypes': '-1',
                    'pageSize': 500,
                    'pageNumber': i,
                    'reportName': 'RPT_MUTUAL_HOLD_DET',
                    'columns': 'ALL',
                    'source': 'WEB',
                    'client': 'WEB',
                    'filter': f'(PARTICIPANT_CODE="{name}")' + \
                        f'(MARKET_CODE in ("001","003"))(HOLD_DATE=\'{date}\')',
                }
                headers = {
                    "Referer": "https://data.eastmoney.com/"
                }
                res = Request(url=cls.__data_center, headers=headers, params=params).get().response
                data = eval(res.text.replace('true', 'True').replace('false', 'False').\
                    replace('null', 'np.nan').replace(callbackfunc, '')[1:-2])
                if data['result'] is not np.nan:
                    data = pd.DataFrame(data['result']['data'])
                    datas.append(data)
                else:
                    break
            
        datas = pd.concat(datas, axis=0).reset_index(drop=True)
        datas = datas.rename(columns=dict(zip(datas.columns, datas.columns.map(lambda x: x.lower()))))
        datas.hold_date = pd.to_datetime(datas.hold_date)
        return datas
    
    @classmethod
    @Cache(prefix='Em_stock_buyback', expire_time=2592000)
    def stock_buyback(cls, date: 'str | datetime.datetime') -> pd.DataFrame:
        date = time2str(date)
        datas = []
        for i in range(50):
            params = {
                'sortColumns': 'dim_date',
                'sortTypes': -1,
                'pageSize': 500,
                'pageNumber': i,
                'reportName': 'RPTA_WEB_GETHGLIST',
                'columns': 'ALL',
                'source': 'WEB',
            }
            headers = {
                "Referer": "https://data.eastmoney.com/"
            }
            res = Request(cls.__data_center, headers=headers, params=params).get().json
            if res['result'] is None:
                break
            data = res['result']['data']
            data = pd.DataFrame(data)
            datas.append(data)
        datas = pd.concat(datas, axis=0).reset_index(drop=True)
        datas = datas.rename(columns=dict(zip(datas.columns, datas.columns.map(lambda x: x.lower()))))
        datas.repurenddate = pd.to_datetime(datas.repurenddate)
        datas.repurstarrtdate = pd.to_datetime(datas.repurstartdate)
        datas.updatedate = pd.to_datetime(datas.updatedate)
        datas.dim_date = pd.to_datetime(datas.dim_date)
        datas.dim_tradedate = pd.to_datetime(datas.dim_tradedate)
        datas = datas.loc[datas['dim_date'] == date]
        return datas


class Guba:
    __root = "http://guba.eastmoney.com"
    
    @staticmethod
    def clean_html_text(text: str):
        return text.strip().replace('\r\n', '').replace(' ', '')

    @classmethod
    @Cache(prefix='Guba_overview_info', expire_time=18000)
    def overview_info(cls, code: str, page: int):
        page = str(page)
        url = f"{cls.__root}/list,{code},f_{page}.html"
        html = ProxyRequest(url).get().etree
        read = html.xpath('//*[@id="articlelistnew"]/div[not(@class="dheader")]/span[1]/text()')
        comments = html.xpath('//*[@id="articlelistnew"]/div[not(@class="dheader")]/span[2]/text()')
        title = html.xpath('//*[@id="articlelistnew"]/div[not(@class="dheader")]/span[3]/a/text()')
        href = html.xpath('//*[@id="articlelistnew"]/div[not(@class="dheader")]/span[3]/a/@href')
        author = html.xpath('//*[@id="articlelistnew"]/div[not(@class="dheader")]/span[4]/a/font/text()')
        datetime = html.xpath('//*[@id="articlelistnew"]/div[not(@class="dheader")]/span[5]/text()')
        data = pd.DataFrame({"read": read, "comments": comments, "title": title, "href": href, "author": author, "datetime": datetime})
        return data

    @classmethod
    @Cache(prefix='Guba_detail_info_orig', expire_time=18000)
    def _detail_info_orig(cls, url: str):
        html = ProxyRequest(url).get().etree
        content = ''.join(html.xpath('//*[@id="zwconbody"]//text()'))
        datetime_source = ''.join(html.xpath('//*[@id="zwconttb"]/div[@class="zwfbtime"]/text()'))
        data = [cls.clean_html_text(content), cls.clean_html_text(datetime_source)]
        return data
    
    @classmethod
    @Cache(prefix='Guba_detail_info_cfh', expire_time=18000)
    def _detail_info_cfh(cls, url: str):
        html = ProxyRequest(url).get().etree
        content = ''.join(html.xpath('//*[@class="article-body"]//text()'))
        datetime_source = ''.join(html.xpath('//*[@class="article-meta"]//text()'))
        data = [cls.clean_html_text(content), cls.clean_html_text(datetime_source)]
        return data
    
    @classmethod
    @Cache(prefix='Guba_detail_info', expire_time=18000)
    def detail_info(cls, code: str, page: int):
        brief_data = cls.overview_info(code, page).set_index('href')
        detail_data = []
        for url in brief_data.index:
            data = cls._detail_info(url)
            detail_data.append(data)
        detail_data = pd.DataFrame(detail_data, columns=['content', 'datetime_source'], index=brief_data.index)
        result_data = pd.concat([brief_data, detail_data], axis=1).reset_index()
        return result_data
    
    @classmethod
    def _detail_info(cls, url: str):
        if url.startswith('//caifuhao'):
            return cls._detail_info_cfh('http:' + url)
        else:
            return cls._detail_info_orig(cls.__root + url)
    
    @classmethod
    @Cache(prefix='Guba_detail_info_', expire_time=18000)
    def detail_info_(cls, code: str, page: int):
        # On MacOS, if you want to successfully run this function,
        # you may need to set a environment variable
        # export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
        # see https://blog.csdn.net/hanshuobest/article/details/104220412
        brief_data = cls.overview_info(code, page).set_index('href')
        result = async_job(brief_data.index, cls._detail_info)
        result_data = pd.DataFrame(result.values(), index=result.keys(), 
            columns=['content', 'datetime_source'])
        result = pd.concat([brief_data, result_data], axis=1).reset_index()
        return result
        