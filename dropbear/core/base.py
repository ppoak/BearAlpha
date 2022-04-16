import re
import random
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from copy import deepcopy
from typing import Iterable
from pandas import ExcelWriter


class Drawer(object):
    '''A Parameter Class for Drawing'''
    def __init__(self, method: str = 'line', date: 'str | list | slice' = slice(None),
        asset: 'str | list | slice' = slice(None), name: str = None, 
        indicator: 'str | list | slice' = slice(None), title: str = 'Image', ax: plt.Axes = None, **kwargs):
        self.method = method
        self.date = date
        self.indexer = eval(str([(date, asset), (name, indicator)]).replace('None, ', ''))
        self.name = name
        self.ax = ax
        self.kwargs = kwargs
        self.title = title
        if not(self.is_ts or self.is_cs):
            raise TypeError('indexer is wrongly set, please check')
    
    @property
    def is_ts(self) -> bool:
        if isinstance(self.date, (slice, list)):
            return True
        else:
            return False
    
    @property
    def is_cs(self) -> bool:
        if isinstance(self.date, (str, tuple)):
            return True
        else:
            return False

    @property
    def unstack_level(self) -> str:
        if self.is_ts:
            return 'asset'
        else:
            return 'datetime'

class Data():
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

    def __init__(self, *args, name: 'str' = 'data', **kwargs):
        '''Standarized data used in dropbear framework
        ----------------------------------------------
        
        name: str, the name of the data
        args: list of DataFrame or Series, but they must be in MultiIndex form,
            because if data in single index, the name of the frame or series 
            will be lost
        kwargs: dict of DataFrame or Series, they can in single index form
        '''
        self.name = name

        data_dict = {}
        for arg in args:
            if isinstance(arg, (pd.DataFrame, pd.Series)):
                if self.__index_dim(arg) > 1:
                    data_dict.update(self.__process_multiindex(arg))
                else:
                    raise TypeError('If single index dataframe is pass, please use keyword argument')
            else:
                raise TypeError('DataFrame or Series with Multiindex is required')
            
        for key, value in kwargs.items():
            if isinstance(value, (pd.Series, pd.DataFrame)):
                key = self.daterange_key(key)
                if isinstance(value, pd.Series):
                    value = value.to_frame()
                data_dict[key] = value
            else:
                raise TypeError('DataFrame or Series is required')

        idx_dims = []
        for name, data in data_dict.items():
            idx_dims.append(self.__index_dim(data))
            data.index.name = 'datetime'
            data.columns.name = 'asset'
        if len(set(idx_dims)) > 1:
            print("[!] Some dataframe isn't consistent with the index dimension, "
                "not that data.df will no longer be availale\n"
                f"index dimensions in Data: {self.name} {idx_dims}")

        self.data_dict = data_dict

    def __process_multiindex(self, data: 'pd.DataFrame | pd.Series') -> 'dict':
        '''multiindex data process'''
        if isinstance(data, pd.Series):
            data = data.to_frame()
        data_dict = {}
        for col in data.columns:
            data_dict[col] = data[col].unstack()
        return data_dict

    def __index_dim(self, data: 'pd.DataFrame | pd.Series') -> int:
        '''return the index dimension number of given data'''
        try:
            dim = len(data.index.levshape)
        except:
            dim = 1
        return dim

    def get(self, key: 'str', default: 'any') -> 'pd.DataFrame':
        return self.dic.get(key, default)

    def copy(self) -> 'Data':
        return deepcopy(self)
    
    def items(self) -> 'Iterable':
        return self.dic.items()
    
    def daterange_key(self, key: 'str') -> 'str':
        match = re.match(r'([ymwdYMWD])(\d+)', key)
        if match:
            return match.group(2) + match.group(1)
        else:
            return key

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
    def dic(self) -> 'dict':
        return self.data_dict
    
    @property
    def df(self) -> 'pd.DataFrame':
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

    def __getitem__(self, key: 'str') -> pd.DataFrame:
        return self.data_dict[key]

    def __repr__(self) -> 'str':
        return f'{self.name}: {list(self.data_dict.keys())}'
    
    def __str__(self) -> 'str':
        return self.__repr__()
    
    def __len__(self) -> 'int':
        return len(self.dic)

    def __bool__(self) -> bool:
        return self.df.empty

class DataCollection():
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
            if isinstance(drawers, Drawer):
                drawers = [drawers]
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
    

if __name__ == "__main__":
    a = pd.DataFrame(np.random.rand(100, 5), index=pd.date_range('20210101', periods=100),
        columns=['a', 'b', 'c', 'd', 'e'])
    b = pd.DataFrame(np.random.rand(500, 5), index=pd.MultiIndex.from_product(
        [pd.date_range('20210101', periods=100), list('abcde')]),
        columns=['id1', 'id2', 'id3', 'id4', 'id5'])
    c = pd.Series(np.random.rand(500), index=pd.MultiIndex.from_product(
        [pd.date_range('20210101', periods=100), list('abcde')]), name='id6')
    data0 = Data(b, c, id7=a)
    data1 = Data(id8=a, name='23333')
    collection = DataCollection(data0, data1)
    # collection.to_file('test.xlsx') # data.to_file('test.csv')
    # data.to_file('test.xlsx')
    # data0.draw(Drawer(method='bar', indexer=[(slice(None), 'a'), 'id6']))
    # collection._draw(
    #     Drawer(method='bar', asset='a', name='data', indicator='id2')
    # )
    collection.gallery(
        Drawer('line', asset='a', name='data', indicator='id1', title='a_company_for_id1'),
        Drawer('bar', date='20210101', name='data', indicator='id7', title='all_company_on_20210101_for_id7'),
        Drawer('line', asset='c', name='data', indicator=['id3', 'id4'], title='c_company_for_id3_id4'),
        Drawer('bar', date=['20210102', '20210107'], asset=['a', 'c'], name=['data', '23333'] ,indicator=['id2', 'id7'], title='a_c_company_on_20210101_20210102_for_id2_id7'),
        shape=(2, 2)
    )
    