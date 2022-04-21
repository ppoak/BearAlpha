import numpy as np
import pandas as pd
from ..tools import *


def merge(left: 'pd.DataFrame | pd.Series | PanelFrame', right: 'pd.DataFrame | pd.Series | PanelFrame', 
    **kwargs) -> 'pd.DataFrame | pd.Series | PanelFrame':
    '''A dummy function for pandas.merge, if both left and right are PanelFrame,
    then return a PanelFrame, otherwise return a pd.DataFrame'''
    if isinstance(left, PanelFrame) and isinstance(right, PanelFrame):
        return PanelFrame(dataframe=pd.merge(left, right, **kwargs))
    else:
        return pd.merge(left, right, **kwargs)

def concat(objs: 'pd.DataFrame | pd.Series | PanelFrame', **kwargs) -> 'pd.DataFrame | pd.Series | PanelFrame':
    '''A dummy function for pandas.concat, if all objects are PanelFrame, 
    then return a PanelFrame, otherwise return a pd.DataFrame'''
    if all(list(map(lambda x: isinstance(x, PanelFrame), objs))):
        return PanelFrame(dataframe=pd.concat(objs, **kwargs))
    else:
        return pd.concat(objs, **kwargs)

class PreProcessor_(object):
    '''Data PreProcessor, using to preprocess data before passing panel data to calculation
    ======================================================================================

    This is a Class used to preprocess data before passing panel data to calculation.
    '''

    def __init__(self, panel: PanelFrame):
        self.panel = panel
    
    def deextreme(self, method: str = "mean_std", n: 'int | list' = 5) -> PanelFrame:
        """deextreme data
        ----------------

        method: str, must be one of these:
            'median': median way
            'fix_odd': fixed odd way
            'mean_std': mean_std way
        n: int or list, 
            when using median, n is the number of deviations from median
            when using fixed odd percentile, n must be one of those:
                1. the remaining percentiles for head and tail in a list form
                2. the n percentiles for head and tail in a float form
            when using mean std, ni is the number of deviations from mean

        return: PanelFrame, result
        """
        def _mean_std(data, n):
            mean = data.mean()
            std = data.std()
            data = data.copy()
            up = mean + n * std
            down = mean - n * std
            data = data.clip(down, up, axis=1)
            return data
        
        def _median_correct(data, n):
            ''''''
            md = data.median()
            mad = (data - md).abs().median()
            up = md + n * mad
            down = md - n * mad
            data = data.copy()
            data = data.clip(down, up, axis=1)
            return data
        
        def _fix_odd(data, n):
            if isinstance(n, (list, tuple)):
                data = data.copy()
                data = data.clip(data.quantile(n[0]), data.quantile(n[1]), axis=1)
                return data
            else:
                data = data.copy()
                data = data.clip(data.quantile(n / 2), data.quantile(1 - n / 2), axis=1)
                return data

        data = self.panel.groupby(level=0).apply(eval(f'_{method}'), n=n)
        return PanelFrame(dataframe=data)
        
    def standard(self, method: str = 'zscore'):
        """standardization ways
        -----------------

        method: str, standardization type must be one of these:
            'zscore': z-score standardization
            'weight': weight standardization
            'rank': rank standardization
        return: np.array, 标准化后的因子数据
        """
        def _zscore(data):
            mean = data.mean()
            std = data.std()
            data = (data - mean) / std
            return data
        
        data = self.panel.groupby(level=0).apply(eval(f'_{method}'))        
        return PanelFrame(dataframe=data)
    
    def missing_fill(self, method: str = 'fillzero'):
        """missing fill ways
        ---------------------

        data: np.array, raw data
        method: choices between ['drop', 'zero']
        return: np.array, missing data filled with 0
        """
        def _dropna(data):
            return data.dropna()

        def _fillzero(data):
            data = data.copy()
            data = data.fillna(0)
            return data
        
        def _fillmean(data):
            data = data.copy()
            data = data.fillna(data.mean())
            return data
        
        def _fillmedian(data):
            data = data.copy()
            data = data.fillna(data.median())
            return data

        data = self.panel.groupby(level=0).apply(eval(f'_{method}'))
        return PanelFrame(dataframe=data)

@pd.api.extensions.register_dataframe_accessor("preprocessor")
class PreProcessor(Worker):
    
    def price2ret(self, period: str, open_column: str = 'close', close_column: str = 'close'):
        if self.type_ == Worker.PANEL:
            # https://pandas.pydata.org/docs/reference/api/pandas.Grouper.html
            # https://stackoverflow.com/questions/15799162/
            close_price = self.dataframe.groupby([
                pd.Grouper(level=0, freq=period, label='right'),
                pd.Grouper(level=1)
            ]).last().loc[:, close_column]
            open_price = self.dataframe.groupby([
                pd.Grouper(level=0, freq=period, label='right'),
                pd.Grouper(level=1)
            ]).first().loc[:, open_column]

        elif self.type_ == Worker.TIMESERIES:
            close_price = self.dataframe.resample(period, label='right')\
                .last().loc[:, close_column]
            open_price = self.dataframe.resample(period, label='right')\
                .first().loc[:, open_column]

        return (close_price - open_price) / open_price

    def price2fwd(self, period: str, open_column: str = 'open', close_column: str = 'close'):
        if self.type_ == Worker.PANEL:
            # https://pandas.pydata.org/docs/reference/api/pandas.Grouper.html
            # https://stackoverflow.com/questions/15799162/
            close_price = self.dataframe.groupby([
                pd.Grouper(level=0, freq=period, label='left'),
                pd.Grouper(level=1)
            ]).last().loc[:, close_column]
            open_price = self.dataframe.groupby([
                pd.Grouper(level=0, freq=period, label='left'),
                pd.Grouper(level=1)
            ]).first().loc[:, open_column]

        elif self.type_ == Worker.TIMESERIES:
            close_price = self.dataframe.resample(period, label='left')\
                .last().loc[:, close_column]
            open_price = self.dataframe.resample(period, label='left')\
                .first().loc[:, open_column]

        return (close_price - open_price) / open_price
        
    def cum2diff(self, grouper = None, period: int = 1, axis: int = ..., keep: bool = True):
        def _diff(data):
            diff = data.diff(period, axis=axis)
            if keep:
                diff.iloc[:period] = data.iloc[period:]
        
        if grouper is None:
            diff = _diff(self.dataframe)
        else:
            diff = self.dataframe.groupby(grouper).apply(_diff)
            
        return diff

    def dummy2category(self, dummy_columns, name: str = 'group'):
        columns = pd.DataFrame(
            dummy_columns.values.reshape((1, -1))\
            .repeat(self.dataframe.shape[0], axis=0),
            index=self.dataframe.index, columns=self.dataframe.columns
        )
        category = columns[self.dataframe.loc[:, dummy_columns].astype('bool')]\
            .replace(np.nan, '').astype('str').sum(axis=1)
        category.name = name
        return category

    def logret2algret(logret):
        return np.exp(logret) - 1
    
    def algret2logret(algret):
        return np.log(algret + 1)


if __name__ == "__main__":
    import numpy as np
    price = pd.DataFrame(np.random.rand(500, 4), columns=['open', 'high', 'low', 'close'],
        index=pd.MultiIndex.from_product([pd.date_range('20100101', periods=100), list('abced')]))
    