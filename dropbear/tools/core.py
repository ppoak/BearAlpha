import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pandas import ExcelWriter


class PanelFrame(pd.DataFrame):
    '''A Panel Data Class Based On Pandas MultiIndex DataFrame
    ==========================================================

    Panel Data is composed by A Series of DataFrame, it can be viewed in
    three dimensions:

    1. indicator; 2. asset; 3. datetime

    So, we can construct a PanelFrame by passing through a single way
    of viewing data, like indicators = {...} or assets = {...} or datetimes = {...}
    
    However, there are some requirements while constructing this Panel:

    1. if you passing data from the indicator view, the data should be a dict
        with indicator name as key and a DataFrame as value, the DataFrame
        index must be DatetimeIndex, and the DataFrame columns must be assets names
    2. if you passing data from the asset view, the data should be a dict
        with asset name as key and a DataFrame as value, the DataFrame
        index must be DatetimeIndex, and the DataFrame columns must be indicators names
    3. if you passing data from the datetime view, the data should be a dict
        with datetime name as key and a DataFrame as value, the DataFrame
        index must be assets names, and the DataFrame columns must be indicators names
    4. or more simply, you can pass a DataFrame, the DataFrame index must be
        multi index, the first level must be datetime, the second level must be assets,
        and the columns must be indicators
    '''

    def __init__(self, indicators: dict = None, 
        assets: dict = None, datetimes: dict = None,
        dataframe: pd.DataFrame = None, **kwargs):
        '''Create a Panel DataFrame
        ----------------------------
        
        indicators: dict, giving data in a indicator vision, the dictionary must conform 
            <indicator name>: <DataFrame> (DataFrame index must be DatetimeIndex)
        assets: dict, giving data in a asset vision, the dictionary must conform
            <asset name>: <DataFrame> (DataFrame index must be DatetimeIndex)
        datetimes: dict, giving data in a datetime vision, the dictionary must conform 
            <datetime name>: <DataFrame> (DataFrame index must be assets names)
        data: pd.DataFrame, if data is passed, it must be a multi index DataFrame,
            index level0 being datetime, index level1 being assets, columns being indicators
        kwargs: some keyword arguments passed to create the DataFrame
        '''

        status = (indicators is not None, assets is not None, datetimes is not None, dataframe is not None)
        data = []
        if status == (False, False, False, False):
            raise ValueError('at least one of assets, indicators or dates should be passed!')

        elif status == (False, False, False, True):
            data = dataframe
            
        elif status == (True, False, False, False):
            for name, indicator in indicators.items():
                if not isinstance(indicator.index, pd.DatetimeIndex):
                    raise IndexError('indicator index should be a DatetimeIndex!')
                indicator = indicator.stack()
                data.append(indicator)
            data = pd.concat(data, axis=1)
            data.columns = indicators.keys()

        elif status == (False, True, False, False):
            for name, asset in assets.items():
                if not isinstance(asset.index, pd.DatetimeIndex):
                    raise IndexError('asset index should be a DatetimeIndex!')
                asset.index = pd.MultiIndex.from_product([asset.index, [name]])
                data.append(asset)
            data = pd.concat(data).sort_index()
            
        elif status == (False, False, True, False):
            data =[]
            for name, datetime in datetimes.items():
                datetime.index = pd.MultiIndex.from_product([pd.to_datetime([name]), datetime.index])
                data.append(datetime)
            data = pd.concat(data).sort_index()

        else:
            raise ValueError('only one of assets, indicators or dates should be passed, or just pass a dataframe!')
        
        # mind that before initializing super() class, we cannot set any attributes
        super().__init__(data.values, index=data.index, columns=data.columns, **kwargs)
        # after initialization, we can set some data container as normal class

    @property
    def dict(self) -> dict:
        '''the dictionary form of the PanelFrame'''
        data_dict = {}
        for col in self.columns:
            data_dict[col] = self[col].unstack(level=1)
        return data_dict
    
    @property
    def levshape(self) -> tuple:
        '''PanelFrame level shape, (datetime unique number, asset unique number, indicator unique number)'''
        return self.index.levshape + (len(self.columns),)
    
    @property
    def indicators(self):
        '''The indicators of the PanelFrame'''
        return self.columns
    
    @property
    def datetimes(self):
        '''The datetimes of the PanelFrame'''
        return self.index.levels[0]
    
    @property
    def assets(self):
        '''The assets of the PanelFrame'''
        return self.index.levels[1]
    
    def to_excel(self, path, multisheet: bool = True, **kwargs):
        '''Export to Excel
        -----------------
        
        path: str, the path to save the excel file
        multisheet: bool, whether to export to multiple sheets
        **kwargs: some keyword arguments passed to pandas.DataFrame.to_excel
        '''
        if multisheet:
            with ExcelWriter(path) as writer:
                for col in self.columns:
                    self[col].unstack(level=1).to_excel(writer, sheet_name=col)
        else:
            super().to_excel(path, **kwargs)
    
    def tcorr(self, method: str = 'pearson', tvalue: bool = True) -> 'pd.DataFrame | PanelFrame':
        '''Give the Correspondant Coefficient t value During Time Series
        ---------------------------------------------------------------

        method: str, the method to calculate the corr value across the time span,
        tvalue: whether to return the t value
        return: pd.DataFrame, the t value  of the PanelFrame across the panel time span
        '''
        corr_series = self.groupby(level=0).corr(method=method)
        if not tvalue:
            return PanelFrame(dataframe=corr_series)
        n = corr_series.index.levels[0].size
        corr_mean = corr_series.groupby(level=1).mean()
        corr_std = corr_series.groupby(level=1).std()
        return (corr_mean / corr_std).replace(np.inf, np.nan) * np.sqrt(n - 1)
    
    def cut(self, datetime: str = slice(None), asset: str = slice(None),
        indicator: str = slice(None)) -> 'pd.DataFrame | pd.Series | any':
        '''Cut the PanelFrame into Cross Section DataFrame or Time Series DataFrame
        ---------------------------------------------------------------------------
        
        datetime: str, the datetime to cut
        asset: str, the asset to cut
        indicator: str, the indicator to cut
        return: pd.DataFrame or pd.Series or Any, the cross section dataframe or time series dataframe
        '''
        status = (isinstance(datetime, str), isinstance(asset, str), isinstance(indicator, str))
        data = self.loc[(datetime, asset), indicator].copy()
        if status == (False, False, False):
            raise IndexError('at least one of datetime, asset or indicator should be str type!')
        elif status[0]:
            data = data.droplevel(0)
        elif status[1]:
            data = data.droplevel(1)
        elif status[2]:
            data = data.unstack(level=1)
        return data

    def draw(self, kind: str, datetime: str = slice(None), 
        asset: str = slice(None), indicator: str = slice(None), **kwargs):
        '''Draw the Cross Section or Time Series DataFrame
        -------------------------------------------------

        kind: str, the kind of plot, can be 'line', 'bar', 'hist', 'box', 'kde', 'area', 'scatter'
        datetime: str, the datetime to draw
        asset: str, the asset to draw
        '''
        data = self.cut(datetime, asset, indicator)
        if not isinstance(data, (pd.Series, pd.DataFrame)):
            raise ValueError('Your cut seems to be a number instead of Series or DataFrame')
        if isinstance(datetime, str):
            data.plot(kind=kind, **kwargs)
        else:
            data.index = data.index.strftime(r'%Y-%m-%d')
            data.plot(kind=kind, **kwargs)


if __name__ == "__main__":
    import numpy as np
    import matplotlib.pyplot as plt

    indicators = dict(zip(
        [f'indicator{i + 1}' for i in range(5)],
        [pd.DataFrame(np.random.rand(100, 5), index=pd.date_range('2020-01-01', periods=100), columns=list('abcde')) for _ in range(5)]
        ))
    assets = dict(zip(
        list('abcde'),
        [pd.DataFrame(np.random.rand(100, 5), index=pd.date_range('2020-01-01', periods=100), columns=[f'indicator{i+1}' for i in range(5)]) for _ in range(5)]
    ))    
    datetimes = dict(zip(
        pd.date_range('2020-01-01', periods=100),
        [pd.DataFrame(np.random.rand(5, 5), index=list('abcde'), columns=[f'indicator{i+1}' for i in range(5)]) for _ in range(100)]
    ))

    pfi = PanelFrame(indicators=indicators)
    pfa = PanelFrame(assets=assets)
    pfd = PanelFrame(datetimes=datetimes)
    
    week_increment = pfi.csdiff('%w')
    print(week_increment)
    
    print('=' * 20 + ' PanelFrame ' + '=' * 20)
    print(pfi)
    print(pfa)
    print(pfd)

    print('=' * 20 + ' PanelFrame attributes ' + '=' * 20)
    print(pfi.datetimes, pfi.assets, pfi.indicators)
    print(pfa.datetimes, pfa.assets, pfa.indicators)
    print(pfd.datetimes, pfd.assets, pfd.indicators)

    print('=' * 20 + ' PanelFrame data ' + '=' * 20)
    print(pfi.levshape, pfi.dict)
    print(pfa.levshape, pfa.dict)
    print(pfd.levshape, pfd.dict)

    pfi.to_excel('pfi.xlsx')
    pfa.to_excel('pfa.xlsx')
    pfd.to_excel('pfd.xlsx')

    print('=' * 20 + ' PanelFrame corr ' + '=' * 20)
    print(pfi.tcorr())
    print(pfa.tcorr())
    print(pfd.tcorr())

    print('=' * 20 + ' PanelFrame cut ' + '=' * 20)
    print(pfi.cut(indicator='indicator1'))
    print(pfa.cut(datetime='20200101'))
    print(pfd.cut(asset='a'))

    pfi.draw('line', asset='a', indicator='indicator1', title='Test', figsize=(20, 8))
    pfa.draw('bar', datetime='20200101', indicator=['indicator1', 'indicator2'])
    pfd.draw('hist', datetime='20200104', indicator='indicator4')

    plt.show()
    