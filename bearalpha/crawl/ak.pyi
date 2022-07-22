from bearalpha import *

class AkShare:

    @classmethod
    def market_daily(cls, code: str, start: str = None, end: str = None) -> DataFrame: ...

    @classmethod
    def stock_quote(cls, code_only: bool = False) -> DataFrame: ...

    @classmethod
    def etf_market_daily(cls, code: str, start: str = None, end: str = None) -> DataFrame: ...

    @classmethod
    def stock_fund_flow(cls, code: str) -> DataFrame: ...

    @classmethod
    def stock_fund_rank(cls) -> DataFrame: ...
