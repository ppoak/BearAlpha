import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
# from ..tools import item2list, price2fwd, periodkey
# from ..tools.util import index_dim
from copy import deepcopy
from typing import Iterable
from pandas import ExcelWriter


class PanelFrame(pd.DataFrame):
    '''A Panel Data Class Based On Pandas MultiIndex DataFrame'''

    def __init__(self, indicators: dict = None, assets: dict = None, datetimes: dict = None, **kwargs):
        '''Create a Panel DataFrame
        ----------------------------
        
        indicators: dict, giving data in a indicator vision, the dictionary must conform 
            <indicator name>: <DataFrame> (DataFrame index must be DatetimeIndex)
        assets: dict, giving data in a asset vision, the dictionary must conform
            <asset name>: <DataFrame> (DataFrame index must be DatetimeIndex)
        datetimes: dict, giving data in a datetime vision, the dictionary must conform 
            <datetime name>: <DataFrame> (DataFrame index must be assets names)
        kwargs: some keyword arguments passed to create the DataFrame
        '''

        status = (indicators is not None, assets is not None, datetimes is not None)
        data = []
        if status == (False, False, False):
            raise ValueError('at least one of assets, indicators or dates should be passed!')
            
        elif status == (True, False, False):
            for name, indicator in indicators.items():
                if not isinstance(indicator.index, pd.DatetimeIndex):
                    raise IndexError('indicator index should be a DatetimeIndex!')
                indicator = indicator.stack()
                data.append(indicator)
            data = pd.concat(data, axis=1)
            data.columns = indicators.keys()

        elif status == (False, True, False):
            for name, asset in assets.items():
                if not isinstance(asset.index, pd.DatetimeIndex):
                    raise IndexError('asset index should be a DatetimeIndex!')
                asset.index = pd.MultiIndex.from_product([asset.index, [name]])
                data.append(asset)
            data = pd.concat(data).sort_index()
            
        elif status == (False, False, True):
            data =[]
            for name, datetime in datetimes.items():
                datetime.index = pd.MultiIndex.from_product([[name], datetime.index])
                data.append(datetime)
            data = pd.concat(data)

        else:
            raise ValueError('only one of assets, indicators or dates should be passed!')

        super().__init__(data.values, index=data.index, columns=data.columns, **kwargs)

    @property
    def data(self) -> dict:
        data_dict = {}
        for col in self.columns:
            data_dict[col] = self[col].unstack(level=1)
        return data_dict
    
    @property
    def levshape(self) -> tuple:
        return self.index.levshape + (len(self.columns),)
    
    @property
    def indicators(self):
        return self.columns
    
    @property
    def datetimes(self):
        return self.index.levels[0]
    
    @property
    def assets(self):
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
    
    def tcorr(self, method: str = 'pearson', tvalue: bool = True):
        '''Give the Correspondant Coefficient t value During Time Series'''
        corr_series = self.groupby(level=0).corr(method=method)
        if not tvalue:
            return corr_series
        n = corr_series.index.levels[0].size
        corr_mean = corr_series.groupby(level=1).mean()
        corr_std = corr_series.groupby(level=1).std()
        return (corr_mean / corr_std).replace(np.inf, np.nan) * np.sqrt(n - 1)
    
    def cut(self, datetime: str = slice(None), asset: str = slice(None), indicator: str = slice(None)):
        '''Cut the PanelFrame into Cross Section DataFrame or Time Series DataFrame
        ---------------------------------------------------------------------------
        
        datetime: str, the datetime to cut
        asset: str, the asset to cut
        indicator: str, the indicator to cut
        
        **cut returns a copy of the original data, so it cannot be used to modify the original data
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
  
"""
class Data(object):
    '''This is a Standard Data Set for Containing Different Data
    ===========================================================

    Data can contain different types of data, including:

    1. Pandas DataFrame
    2. Pandas Series
    3. Dictionary

    all of them can be loaded and stored in dropbear framework, 
    the main data type in this class is pandas dataframe, with 
    single index in datetime format, the columns should be assets'
    name. However we can also represent the data in dictionary 
    form or in multi-index form.

    the input data can either be a pandas dataframe, pandas series, 
    or dictionary. if the input data is a multi-index data, the column
    should be the indicator name, and level 0 should be the datetime, 
    level 1 should be the assets name.
    '''                              

    def __init__(self, assets: dict = None, 
        indicators: dict = None, dates: dict = None, 
        name: 'str' = 'data'):
        '''Standarized data used in dropbear framework
        ----------------------------------------------
        
        name: str, the name of the data
        '''
        # set Data name to name
        self.name = name
        if assets is None and indicators is None and dates is None:
            raise ValueError('at least one of assets, indicators or dates should be passed!')
        elif assets is not None and indicators is None and dates is None:
            self.dic = assets
        elif assets is None and indicators is not None and dates is None:
            self.dic = indicators
        elif assets is None and indicators is None and dates is not None:
            self.dic = dates
        else:
            raise ValueError('only one of assets, indicators or dates should be passed!')
        
        # if using *args to pass the parameters, the args should be in MultiIndex form
        for arg in args:
            if isinstance(arg, (pd.DataFrame, pd.Series)):
                if index_dim(arg) > 1:
                    dic.update(self.__process_multiindex(arg))
                else:
                    raise TypeError('If single index dataframe is pass, please use keyword argument')
            else:
                raise TypeError('DataFrame or Series with Multiindex is required')
        
        for key, value in kwargs.items():
            if isinstance(value, (pd.Series, pd.DataFrame)):
                key = periodkey(key)
                if isinstance(value, pd.Series):
                    value = value.to_frame()
                dic[key] = value
            else:
                raise TypeError('DataFrame or Series is required')

        # naming the data in Data.dic
        for data in dic.values():
            data.index.name = 'datetime'
            data.columns.name = 'asset'
        self.dic = dic

        # constructing Data.df, mind that if there are unmatched
        # single index and multiindex data, the constructing will fail
        df = []
        for key, value in dic.items():
            tmp = value.stack()
            tmp.name = key
            df.append(tmp)
        df = pd.concat(df, axis=1)
        df.columns.name = 'indicator'
        self.df = df

    def __process_multiindex(self, data: 'pd.DataFrame | pd.Series') -> 'dict':
        '''multiindex data process'''
        if isinstance(data, pd.Series):
            data = data.to_frame()
        data_dict = {}
        for col in data.columns:
            data_dict[col] = data[col].unstack()
        return data_dict

    def get(self, key: 'str', default: 'any') -> 'pd.DataFrame':
        return self.dic.get(key, default)  

    def corr(self, indicators: 'str | list' = slice(None), dates: 'str | list' = slice(None), 
        assets: 'str | list' = slice(None), ret_tvalue: bool = True) -> pd.DataFrame:
        '''calculate the correlation between indicators or within indicator time series
        ------------------------------------------------

        indicators: list of str, the name of the indicators
        date: str, the date of the correlation
        '''
        if isinstance(indicators, str) and isinstance(dates, str):
            raise TypeError('at least one of the indicators and dates should be list')
        indicators = item2list(indicators)
        dates = item2list(dates)
        assets = item2list(assets)
        data = self.df.loc[(dates, assets), indicators].copy()
        cor = data.groupby('datetime').corr()
        if not ret_tvalue:
            return cor
        else:
            cor_mean = cor.groupby(level='indicator').mean()
            cor_std = cor.groupby(level='indicator').std()
            cor_n = cor.index.get_level_values('datetime').unique().size
            return (cor_mean / (cor_std / np.sqrt(cor_n))).replace(np.inf, np.nan)

    def copy(self) -> 'Data':
        return deepcopy(self)
    
    def items(self) -> 'Iterable':
        return self.dic.items()
    
    def draw(self, *args, ax: plt.Axes = None, path: str = None, show: bool = True, **kwargs):
        '''Draw images on assigned ax with assigned data
        ------------------------------------------------

        args: DrawParam, the parameters for drawing
        ax: matplotlib axes, default none to create new one, the overall 
            ax setting with lower priority than args
        path: str, the path to save the image
        show: bool, whether to show the image
        '''
        def _check_large(d):
            if d.columns.shape[0] > 10:
                print('[!] your number of indicators is too large, it will draw slowly or the image may be undistinguishable')

        if ax is None:
            _, ax = plt.subplots(figsize=(12, 8))

        if not args:
            draw_data = self.df.unstack(level='asset').copy()
            _check_large(draw_data)
            draw_data.plot(ax=ax)
        
        data = self.df.copy(deep=True)

        for arg in args:
            tmp_kwargs = kwargs.copy()
            tmp_kwargs.update(arg.kwargs)
            tmp_ax = arg.ax if arg.ax else ax
            draw_data = data.loc[arg.indexer[0], arg.indexer[1]]
            draw_data.index = draw_data.index.set_levels(
                data.index.get_level_values('datetime').unique().strftime(r'%Y-%m-%d'), level='datetime')
            draw_data = draw_data.unstack(level=arg.unstack_level)
            _check_large(draw_data)
            draw_data.plot(kind=arg.method, ax=tmp_ax, **tmp_kwargs)
            tmp_ax.xaxis.set_major_locator(mticker.MaxNLocator(10))
            tmp_ax.set_title(arg.title)
        
        if path:
            plt.savefig(path)
        if show:
            plt.show()

    def to_file(self, path: 'str | ExcelWriter') -> None:
        '''save data to file
        ------------------
        path: str or excelwriter, the path of the file
        '''
        if isinstance(path, ExcelWriter):
            for key, value in self.data_dict.items():
                    value.to_excel(path, sheet_name=key)
            
        elif path.endswith('csv'):
            self.df.to_csv(path)

        elif path.endswith('xlsx'):
            with ExcelWriter(path) as writer:
                for key, value in self.data_dict.items():
                    value.to_excel(writer, sheet_name=key)
        else:
            raise ValueError(f"The file format {path.split('.')[-1]} is not supported")

    @property
    def dict(self) -> 'dict':
        return self.data_dict
    
    @property
    def frame(self) -> 'pd.DataFrame':
        if not self.data_dict:
            return pd.DataFrame()
        df = []
        for key, value in self.data_dict.items():
            tmp = value.stack()
            tmp.name = key
            df.append(tmp)
        df = pd.concat(df, axis=1)
        df.columns.name = 'indicator'
        return df
        
    @property
    def assets(self) -> 'pd.Index':
        return self.df.index.get_level_values('asset').unique().to_list()
    
    @property
    def indicators(self) -> 'list':
        return list(self.dic.keys())
    
    @property
    def dates(self) -> 'list':
        return self.df.index.get_level_values('datetime').unique().to_list()

    @property
    def shape(self) -> 'tuple':
        return (self.df.index.shape[0], self.df.columns.shape[0])

    def __getitem__(self, key: 'str | int | tuple') -> pd.DataFrame:
        if isinstance(key, str):
            return self.data_dict[key]
        elif isinstance(key, int):
            return self.data_dict[self.indicators[key]]
        elif isinstance(key, tuple):
            return self.df.loc[key]
        else:
            raise KeyError(f'{key} is not either a int or str')

    def __setitem__(self, key: 'str', value):
        self.df.loc[key] = value

    def __repr__(self) -> 'str':
        return f'{self.name}: {list(self.data_dict.keys())}'
    
    def __str__(self) -> 'str':
        return self.__repr__()
    
    def __len__(self) -> 'int':
        return len(self.dic)

    def __bool__(self) -> bool:
        return self.df.empty

class DataCollection(object):
    '''A Collection of the Standarized Data
    =======================================
    
    DataCollection can only contain Data class data,
    and specifically, DataCollection is used for getting ready
    for subsequent analysis.
    '''

    def __init__(self, *args):
        '''DataCollection is used for getting ready for subsequent analysis
        --------------------------------------------------------------------

        args: dict of Data, the key is the name of the data
        '''
        data_dict = {}
        for arg in args:
            data_dict[arg.name] = arg
        self.data_dict = data_dict

    def to_file(self, path: str):
        '''save data to file
        ------------------
        
        path: str, the path of the file
        '''
        if path.endswith('csv'):
            self.df.to_csv(path)

        elif path.endswith('xlsx'):
            with ExcelWriter(path) as writer:
                for value in self.data_dict.values():
                    value.to_file(writer)

        else:
            raise ValueError(f"The file format {path.split('.')[-1]} is not supported")

    def _draw(self, *args, ax: plt.Axes = None, path: str = None, show: bool = True, **kwargs):
        '''Draw images on assigned ax with assigned data
        ------------------------------------------------

        args: DrawParam, the parameters for drawing
        ax: matplotlib axes, default none to create new one, the overall 
            ax setting with lower priority than args
        path: str, the path to save the image
        show: bool, whether to show the image
        '''
        def _check_large(d):
            if d.columns.shape[0] > 10:
                print('[!] your number of indicators is too large, it will draw slowly or the image may be undistinguishable')

        if ax is None:
            _, ax = plt.subplots(figsize=(12, 8))

        if not args:
            draw_data = self.df.unstack(level='asset').copy()
            _check_large(draw_data)
            draw_data.plot(ax=ax)
        
        data = self.df.copy(deep=True)

        for arg in args:
            tmp_kwargs = kwargs.copy()
            tmp_kwargs.update(arg.kwargs)
            tmp_ax = arg.ax if arg.ax else ax
            draw_data = data.loc[arg.indexer[0], arg.indexer[1]]
            draw_data.index = draw_data.index.set_levels(
                data.index.get_level_values('datetime').unique().strftime(r'%Y-%m-%d'), level='datetime')
            draw_data = draw_data.unstack(level=arg.unstack_level)
            _check_large(draw_data)
            draw_data.plot(kind=arg.method, ax=tmp_ax, **tmp_kwargs)
            tmp_ax.xaxis.set_major_locator(mticker.MaxNLocator(10))
            tmp_ax.set_title(arg.title)
        
        if path:
            plt.savefig(path)
        if show:
            plt.show()

    def gallery(self, *args, shape: 'list | tuple' = (1,1), axes: 'list | np.ndarray' = None,
        path: str = None, show: bool = True):
        '''Plot a gallery of images for assigned data
        ---------------------------------------------

        kwargs: parameters should be passed in the form:
            image title = [Drawer(...), ...] | Drawer(...)
        '''
        if not args:
            raise ValueError('At Least One Drawer should be passed in')
        
        if axes is None:
            _, axes = plt.subplots(nrows=shape[0], ncols=shape[1], figsize=(12 * shape[1], 8 * shape[0]))
        axes = np.array(axes).reshape(shape)

        for i, drawers in enumerate(args):
            drawers = item2list(drawers)
            ax = axes[i // shape[1], i % shape[1]]
            self._draw(*drawers, show=False, ax=ax)
        
        if path:
            plt.savefig(path)
        if show:
            plt.show()

    @property
    def dic(self) -> 'pd.DataFrame':
        all_data_df = []
        for value in self.data_dict.values():
            all_data_df.append(value.df)
    
    @property
    def df(self) -> 'pd.DataFrame':
        all_data_df = []
        for key, value in self.data_dict.items():
            value = value.df.copy()
            col = pd.MultiIndex.from_product([[key], value.columns], names=['category', "indicator"])
            value.columns = col
            all_data_df.append(value)
        df = pd.concat(all_data_df, axis=1)
        return df

    @property
    def shape(self) -> 'tuple':
        return tuple(data.shape for data in self.data_dict.values())

    @property
    def names(self) -> 'list':
        return list(self.data_dict.keys())
    
    @property
    def dates(self) -> 'list':
        d = set()
        for n in self.names:
            d = d.union(set(self[n].dates))
        return sorted(list(d))
    
    @property
    def assets(self) -> 'list':
        a = set()
        for n in self.names:
            a = a.union(set(self[n].assets))
        return sorted(list(a))
    
    @property
    def indictors(self) -> 'list':
        i = set()
        for n in self.names:
            i = i.union(set(self[n].indicators))
        return sorted(list(i))
    
    def __getitem__(self, idx: 'tuple') -> 'pd.DataFrame | Data':
        if isinstance(idx, str):
            return self.data_dict[idx]
        elif len(idx) == 2:
            return self.data_dict[idx[0]].dic[idx[1]]
        elif len(idx) == 1:
            return self.data_dict[idx[0]]
    
    def __repr__(self) -> str:
        res =  ''
        for key, value in self.data_dict.items():
            res += f'{key}: {value.__repr__()}\n'
        return res
    
    def __str__(self) -> str:
        return self.__repr__()
    
    def __bool__(self) -> bool:
        return bool(self.data_dict)
    
class AnalyzeData(DataCollection):
    '''A Collection of the Standarized Data Used for Analysis
    ========================================================
    
    AnalyzeData can only contain Data class data,
    and specifically, DataCollection is used for getting ready
    for subsequent analysis.
    '''
    
    def __init__(self, *args,
        factor: 'Data', 
        forward: 'Data' = None, 
        price: 'Data' = None, 
        group: 'Data' = None,
        infer_forward: 'str | list' = None) -> 'None':
        '''DataCollection is used for getting ready for subsequent analysis
        --------------------------------------------------------------------

        factor: Data, the factor data
        forward: Data or None, the forward data
        price: Data or None, the price data
        group: Data or None, the group data
        infer_forward: str or list, the forward data will be inferred from
        '''
        super().__init__(*args)
        infer_forward = item2list(infer_forward)

        if infer_forward is not None and price is not None:
            if forward is not None:
                print(f'[!] forward is not None, infer_forward {infer_forward} cover forward')
            has_close = price.get('close', False)
            has_open = price.get('open', False)
            if len(price) == 1:
                forward = price2fwd(price[0], price[0], infer_forward)
            elif not has_close and not has_open:
                raise ValueError('Ambiguous infer forward, Please calculate manually')
            elif not has_close and has_open:
                print('[!] Not find close price, infer forward from open price')
                forward = price2fwd(price['open'], price['open'], infer_forward)
            elif has_close and not has_open:
                print('[!] Not find open price, use close price to infer forward')
                forward = price2fwd(price['close'], price['close'], infer_forward)
            else:
                forward = price2fwd(price['open'], price['close'], infer_forward)
            forward = Data(name='forward', **forward)

        self.factor = factor
        self.forward = forward if forward is not None else Data(name='forward')
        self.price = price if price is not None else Data(name='price')
        self.group = group if group is not None else Data(name='group')
        self.data_dict.update({
            "factor": self.factor,
            "forward": self.forward,
            "price": self.price,
            "group": self.group
        })

    def __bool__(self) -> bool:
        return self.price.__bool__() or self.forward.__bool__()
"""


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
    
    print('=' * 20 + ' PanelFrame ' + '=' * 20)
    print(pfi)
    print(pfa)
    print(pfd)

    print('=' * 20 + ' PanelFrame attributes ' + '=' * 20)
    print(pfi.datetimes, pfi.assets, pfi.indicators)
    print(pfa.datetimes, pfa.assets, pfa.indicators)
    print(pfd.datetimes, pfd.assets, pfd.indicators)

    print('=' * 20 + ' PanelFrame data ' + '=' * 20)
    print(pfi.levshape, pfi.data)
    print(pfa.levshape, pfa.data)
    print(pfd.levshape, pfd.data)

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

    pfi.draw('line', asset='a', indicator='indicator1')
    # pfa.draw('bar', datetime='20200101', indicator=['indicator1', 'indicator2'])
    # pfd.draw('hist', datetime='20200104', indicator='indicator4')

    plt.show()
    