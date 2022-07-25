from bearalpha import *

class TuShare:

    @classmethod
    def stock_basic(cls, code_only=False) -> DataFrame: ...

    @classmethod
    def market_daily(cls, code: str, start: str = None, end: str = None) -> DataFrame: ...
