import pandas as pd
from ..tools import *


@pd.api.extensions.register_dataframe_accessor("calculator")
class Calculator(Worker):
    
    def  multirolling(self, window: int, func, *args, **kwargs):
        ''''''
        if self.type_ != Worker.PANEL:
            raise TypeError('multirolling only support for panel data')

        datetime_index = self.dataframe.index.level[0]
        result = []
        for i in range(window, datetime_index.size):
            start_date = datetime_index[i - window]
            end_date = datetime_index[i]
            data = self.dataframe.loc[start_date:end_date].copy()
            window_result = func(data, *args, **kwargs)
        result = pd.concat(result)
        return result
    
    def monorolling(self, window: int, func, *args, **kwargs):
        ''''''
        if self.type_ == Worker.PANEL:
            raise TypeError('monorolling only support for time series or cross section data')
        
        result = []
        for win_data in iter(self.dataframe.rolling(window)):
            window_result = func(win_data, *args, **kwargs)
            result.append(window_result)
        result = pd.concat(result)
        return result
    