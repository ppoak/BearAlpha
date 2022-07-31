from bearalpha import *


class Stock(DataBase):

    def market_daily(self, 
        start: str = None, 
        end: str = None,
        code: 'str | list' = None, 
        fields: list = None,
        and_: 'str | list' = None, 
        or_: 'str | list' = None
    ) -> DataFrame:
        '''get market data in daily frequency
        -------------------------------------

        start: datetime or date or str, start date in 3 forms
        end: datetime or date or str, end date in 3 forms
        fields: list, the field names you want to get
        '''

    def plate_info(self, 
        start: str = None, 
        end: str = None, 
        code: 'list | str' = None, 
        fields: list = None,
        and_: 'str | list' = None,
        or_: 'str | list' = None
    ) -> DataFrame:
        '''get plate info in daily frequency
        -------------------------------------

        start: datetime or date or str, start date in 3 forms
        end: datetime or date or str, end date in 3 forms
        fields: list, the field names you want to get
        conditions: list, a series of conditions like "code = '000001.SZ'" listed in a list
        '''
        
    def index_weights(self, 
        start: str = None, 
        end: str = None,
        code: 'str | list' = None, 
        fields: list = None,
        and_: 'str | list' = None,
        or_: 'str | list' = None
    ) -> DataFrame:
        ...

    def instruments(self, 
        code: 'str | list' = None, 
        fields: list = None,
        and_: 'str | list' = None, 
        or_: 'str | list' = None
    ) -> DataFrame:
        ...

    def index_market_daily(self, 
        start: str = None, 
        end: str = None,
        code: 'str | list' = None, 
        fields: list = None,
        and_: 'str | list' = None,
        or_: 'str | list' = None
    ) -> DataFrame:
        ...

    def derivative_indicator(self, 
        start: str = None, 
        end: str = None,
        code: 'str | list' = None, 
        fields: list = None,
        and_: 'str | list' = None,
        or_: 'str | list' = None
    ) -> DataFrame:
        ...


if __name__ == '__main__':
    pass