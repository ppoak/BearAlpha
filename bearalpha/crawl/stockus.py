import datetime
import pandas as pd
from ..core import *
from ..tools import *


class StockUS:
    
    root = "https://api.stock.us/api/v1/"
    category = {
        "金工量化": 8,
    }
    headers = {
            "Host": "api.stock.us",
            "Origin": "https://stock.us"
        }
    todaystr = datetime.datetime.today().strftime(r'%Y%m%d')
            
    @classmethod
    @Cache(prefix='StockUS_index_price', expire_time=2592000)
    def index_price(
        cls, 
        index: str, 
        start: str, 
        end: str
    ):
        start = time2str(start, formatstr=r'%Y%m%d') or '19900101'
        end = time2str(end, formatstr=r'%Y%m%d') or cls.todaystr
        url = cls.root + f"index-price?security_code={index}&start={start}&stop={end}"
        res = Request(url, headers=cls.headers).get().json
        price = pd.DataFrame(res['price'])
        price['date'] = price['date'].astype('datetime64[ns]')
        price = price.set_index('date')
        return price
    
    @classmethod
    @Cache(prefix='StockUS_cn_price', expire_time=2592000)
    def cn_price(
        cls, 
        code: str, 
        start: str = None,
        end: str = None,
    ):
        start = time2str(start, formatstr=r'%Y%m%d') or '19900101'
        end = time2str(end, formatstr=r'%Y%m%d') or cls.todaystr
        url = cls.root + f"cn-price?security_code={code}&start={start}&stop={end}"
        res = Request(url, headers=cls.headers).get().json
        price = pd.DataFrame(res['price'])
        price['date'] = price['date'].astype('datetime64[ns]')
        price = price.set_index('date')
        return price
    
    @classmethod
    @Cache(prefix='StockUS_report_list', expire_time=2592000)
    def report_list(
        cls, 
        category: str = '金工量化', 
        period: str = '1m', 
        q: str = '', 
        org_name: str = '', 
        author: str = '',
        xcf_years: str = '', 
        search_fields: str = 'title',
        page: int = 1, 
        page_size: int = 100
    ):
        '''Get report data within one category
        ---------------------------------------

        category: str, category to search
        period: str, report during this time period
        q: str, search keyword
        org_name: str, search by org_name
        author: str, search by author
        xcf_years: str, search by xcf_years
        search_fields: str, search in fields, support "title", "content", "content_fp"
        page: int, page number
        page_size: int, page size
        '''
        url = cls.root + 'research/report-list'
        params = (f'?category={cls.category[category]}&dates={period}&q={q}&org_name={org_name}'
                  f'&author={author}&xcf_years={xcf_years}&search_fields={search_fields}'
                  f'&page={page}&page_size={page_size}')
        url += params
        res = Request(url, headers=cls.headers).get().json
        data = pd.DataFrame(res['data'])
        data[['pub_date', 'pub_week']] = data[['pub_date', 'pub_week']].astype('datetime64[ns]')
        data.authors = data.authors.map(
            lambda x: ' '.join(list(map(lambda y: y['name'] + ('*' if y['prize'] else ''), x))))
        data = data.set_index('id')
        return data
    
    @classmethod
    @Cache(prefix='StockUS_report_search', expire_time=2592000)
    def report_search(
        cls, period: str = '3m', 
        q: str = '', 
        org_name: str = '', 
        author_name: str = '',
        xcf_years: str = '', 
        search_fields: str = 'title',
        page: int = 1, 
        page_size: int = 100
    ):
        '''Search report in stockus database
        ---------------------------------------

        period: str, report during this time period
        q: str, search keyword
        org_name: str, search by org_name
        author: str, search by author
        xcf_years: str, search by xcf_years
        search_fields: str, search in fields, support "title", "content", "content_fp"
        page: int, page number
        page_size: int, page size
        '''
        url = cls.root + 'research/report-search'
        params = (f'?dates={period}&q={q}&org_name={org_name}&author_name={author_name}'
                  f'&xcf_years={xcf_years}&search_fields={search_fields}&page={page}'
                  f'&page_size={page_size}')
        url += params
        res = Request(url, headers=cls.headers).get().json
        data = pd.DataFrame(res['data'])
        data['pub_date'] = data['pub_date'].astype('datetime64[ns]')
        data.authors = data.authors.map(
            lambda x: ' '.join(list(map(lambda y: y['name'] + ('*' if y['prize'] else ''), x)))
            if isinstance(x, list) else '')
        data = data.set_index('id')
        return data
