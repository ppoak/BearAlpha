import tushare as ts
import pandas as pd
from ..tools import *
from ..core import *


class TuShare:

    def __init__(self, token: str) -> None:
        self.datasource = ts.pro_api(token)
    
    @Cache(prefix='tushare_market_daily', expire_time=259200)
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


# class TuShare(Loader):
    
#     def __init__(self, config) -> None:
#         super().__init__(config)
#         self.func = config['func']
#         self.args = config.get('args', {})

#     def write(self):
#         for arg in Track(list(self.args)):
#             data = None
#             while data is None:
#                 try:
#                     data = self.func(*arg)
#                 except:
#                     pass
#             data.databaser.to_sql(table=self.table, database=self.database)
    
#     def __call__(self):
#         self.write()
#         self.post()
