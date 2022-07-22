import datetime
from bearalpha import *


class Em:

    @classmethod
    def active_opdep(cls, date: 'str | datetime.datetime') -> DataFrame: ...

    @classmethod
    def active_opdep_details(cls, date: 'str | datetime.datetime') -> DataFrame: ...

    @classmethod
    def institution_trade(cls, date: 'str | datetime.datetime') -> DataFrame: ...

    @classmethod
    def oversea_institution_holding(cls, date: 'str | datetime.datetime') -> DataFrame: ...

    @classmethod
    def stock_buyback(cls, date: 'str | datetime.datetime') -> DataFrame: ...
    
class Guba:

    @classmethod
    def overview_info(cls, code: str, page: int) -> DataFrame: ...

    @classmethod
    def detail_info(cls, code: str, page: int) -> DataFrame: ...

    @classmethod
    def detail_info_(cls, code: str, page: int) -> DataFrame: ...
