import pandas as pd
from ..tools import *


@pd.api.extensions.register_dataframe_accessor("calculator")
class Calculator(Worker):
    
    def rolling(self, window: int, func, *args, grouper = None, **kwargs):
        ''''''
        if self.type_ == Worker.TIMESERIES:
            datetime_index = self.dataframe.index
        elif self.type_ == Worker.PANEL:
            datetime_index = self.dataframe.index.levels[0]
        else:
            raise TypeError('rolling only support for panel or time series data')
        
        result = []
        for i in range(window - 1, datetime_index.size):
            window_data = self.dataframe.loc[datetime_index[i - window + 1]:datetime_index[i]].copy()
            window_data.index = window_data.index.remove_unused_levels()
            if grouper is not None:
                window_result = window_data.groupby(grouper).apply(func, *args, **kwargs)
            else:
                window_result = func(window_data, *args, **kwargs)
            result.append(window_result)
        
        result = pd.concat(result)
        return result
