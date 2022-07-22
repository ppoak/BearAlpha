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
    ) -> DataFrame: ...

    @classmethod
    def report_search(
        cls, period: str = '3m', 
        q: str = '', 
        org_name: str = '', 
        author_name: str = '',
        xcf_years: str = '', 
        search_fields: str = 'title',
        page: int = 1, 
        page_size: int = 100
    ) -> DataFrame: ...
