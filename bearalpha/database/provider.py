import pandas as pd
from ..tools import *
from ..core import *


class Stock(DataBase):
    
    def market_daily(self, 
        start: str = None, 
        end: str = None,
        code: 'str | list' = None, 
        fields: list = None,
        and_: 'str | list' = None, 
        or_: 'str | list' = None
    ) -> pd.DataFrame:
        '''get market data in daily frequency
        -------------------------------------

        start: datetime or date or str, start date in 3 forms
        end: datetime or date or str, end date in 3 forms
        fields: list, the field names you want to get
        '''
        return self.get(
            table = 'market_daily',
            start = start,
            end = end,
            date_col = "date",
            code = code,
            code_col = "order_book_id",
            fields = fields,
            index_col = ["date", 'order_book_id'],
            and_ = and_,
            or_ = or_,
        )
  
    def plate_info(self, 
        start: str = None, 
        end: str = None, 
        code: 'list | str' = None, 
        fields: list = None,
        and_: 'str | list' = None,
        or_: 'str | list' = None
    ) -> pd.DataFrame:
        '''get plate info in daily frequency
        -------------------------------------

        start: datetime or date or str, start date in 3 forms
        end: datetime or date or str, end date in 3 forms
        fields: list, the field names you want to get
        conditions: list, a series of conditions like "code = '000001.SZ'" listed in a list
        '''
        return self.get(
            table = 'plate_info',
            start = start,
            end = end,
            date_col = 'date',
            code = code,
            code_col = "order_book_id",
            fields = fields,
            index_col = ['date', 'order_book_id'],
            and_ = and_,
            or_ = or_,
        )

    def index_weights(self, 
        start: str = None, 
        end: str = None,
        code: 'str | list' = None, 
        fields: list = None,
        and_: 'str | list' = None,
        or_: 'str | list' = None
    ) -> pd.DataFrame:
        return self.get(
            table = 'index_weights',
            start = start,
            end = end,
            date_col = 'date',
            code = code,
            code_col = 'index_id',
            fields = fields,
            index_col = ['date', 'index_id', 'order_book_id'],
            and_ = and_,
            or_ = or_,
        )

    def instruments(self, 
        code: 'str | list' = None, 
        fields: list = None,
        and_: 'str | list' = None, 
        or_: 'str | list' = None
    ) -> pd.DataFrame:
        return self.get(
            table = 'instruments',
            start = None,
            end = None,
            date_col = None,
            code = code,
            code_col = 'order_book_id',
            fields = fields,
            index_col = 'order_book_id',
            and_ = and_,
            or_ = or_,
        )

    def index_market_daily(self, 
        start: str = None, 
        end: str = None,
        code: 'str | list' = None, 
        fields: list = None,
        and_: 'str | list' = None,
        or_: 'str | list' = None
    ) -> pd.DataFrame:
        return self.get(
            table = 'index_market_daily',
            start = start,
            end = end,
            date_col = 'date',
            code = code,
            code_col = 'order_book_id',
            fields = fields,
            index_col = ['date', 'order_book_id'],
            and_ = and_,
            or_ = or_,
        )

    def derivative_indicator(self, 
        start: str = None, 
        end: str = None,
        code: 'str | list' = None, 
        fields: list = None,
        and_: 'str | list' = None,
        or_: 'str | list' = None
    ) -> pd.DataFrame:
        return self.get(
            table = 'derivative_indicator',
            start = start,
            end = end,
            date_col = 'date',
            code = code,
            code_col = 'order_book_id',
            fields = fields,
            index_col = ['date', 'order_book_id'],
            and_ = and_,
            or_ = or_,
        )


class TuShare:

    def __init__(self, token: str) -> None:
        import tushare as ts
        self.datasource = ts.pro_api(token)
    
    def market_daily(self, 
        start: str = None, 
        end: str = None, 
        code: str = None,
    ):
        start = time2str(start).replace('-', '') if start is not None else None
        end = time2str(end).replace('-', '') if end is not None else None
        data = self.datasource.daily(start_date=start, end_date=end, code=code)
        if data.empty:
            return None
        data.trade_date = pd.to_datetime(data.trade_date)
        data = data.set_index(['trade_date', 'ts_code'])
        return data


if __name__ == '__main__':
    pass