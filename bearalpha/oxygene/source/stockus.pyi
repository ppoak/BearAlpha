from bearalpha import *


class StockUS:

    @classmethod
    def index_price(
        cls, 
        index: str, 
        start: str, 
        end: str
    ) -> DataFrame: ...

    @classmethod
    def cn_price(
        cls, 
        code: str, 
        start: str = None,
        end: str = None,
    ) -> DataFrame: ...

    @classmethod
    def quant_report(
        cls, 
        keyword: str = '', 
        period: str = 'all', 
        org_name: str = '', 
        author: str = '',
        xcf_years: str = '', 
        search_fields: str = 'title',
        page: int = 1, 
        page_size: int = 100
    ) -> DataFrame:
        '''Get report data in quant block
        ---------------------------------------

        keyword: str, key word to search, default empty string to list recent 100 entries
        period: str, report during this time period
        q: str, search keyword
        org_name: str, search by org_name
        author: str, search by author
        xcf_years: str, search by xcf_years
        search_fields: str, search in fields, support "title", "content", "content_fp"
        page: int, page number
        page_size: int, page size
        '''
    
    @classmethod
    def search(
        cls, 
        keyword: str = '', 
        period: str = '3m', 
        org_name: str = '', 
        author_name: str = '',
        xcf_years: str = '', 
        search_fields: str = 'title',
        page: int = 1, 
        page_size: int = 100
    ) -> DataFrame:
        '''Search report in stockus database
        ---------------------------------------

        keyword: str, key word to search, default empty string to list recent 100 entries
        period: str, report during this time period
        org_name: str, search by org_name
        author: str, search by author
        xcf_years: str, search by xcf_years
        search_fields: str, search in fields, support "title", "content", "content_fp"
        page: int, page number
        page_size: int, page size
        '''
