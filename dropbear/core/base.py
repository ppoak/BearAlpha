import re
import numpy as np
import pandas as pd
from copy import deepcopy
from typing import Iterable


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

    def __init__(self, name: 'str' = 'data', *args, **kwargs):
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
                if isinstance(arg.index, pd.MultiIndex):
                    data_dict.update(self.__process_multiindex(arg))
                else:
                    raise TypeError('If single index dataframe is pass, please use keyword argument')
            else:
                raise TypeError('DataFrame or Series with Multiindex is required')
            
        for key, value in kwargs.items():
            if isinstance(value, (pd.Series, pd.DataFrame)):
                if isinstance(value.index, pd.MultiIndex):
                    data_dict.update(self.__process_multiindex(value))
                elif isinstance(value.index, pd.DatetimeIndex):
                    key = self.daterange_key(key)
                    data_dict[key] = value
                else:
                    raise TypeError("Only Series, DataFrame type is available")

        for name, data in data_dict.items():
            data.index.name = 'datetime'
            data.columns.name = 'asset'
        self.data_dict = data_dict

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
    def indicators(self) -> 'list':
        return list(self.dic.keys())
    
    @property
    def shape(self) -> 'tuple':
        return (len(self.df.index.levels[0]), len(self.df.columns))

    def __getitem__(self, key: 'str') -> pd.DataFrame:
        return self.data_dict[key]

    def __repr__(self) -> 'str':
        return f'Data: {list(self.data_dict.keys())}'
    
    def __str__(self) -> 'str':
        return self.__repr__()
    
    def __len__(self) -> 'int':
        return len(self.dic)

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

    @property
    def dic(self) -> 'pd.DataFrame':
        all_data_dfi = []
        for key, value in self.data_dict.items():
            all_data_dfi.append(value.dfi)
    
    @property
    def df(self) -> 'pd.DataFrame':
        all_data_df = []
        for key, value in self.data_dict.items():
            value = value.df.copy()
            col = pd.MultiIndex.from_product([[key], value.columns], names=[key, "indicator"])
            value.columns = col
            all_data_df.append(value)
        df = pd.concat(all_data_df, axis=1)
        return df

    @property
    def shape(self) -> 'tuple':
        return tuple(data.shape for data in self.data_dict.values())

    def __getitem__(self, idx: 'tuple') -> 'pd.DataFrame | Data':
        if len(idx) == 2:
            category = idx[0]
            key = idx[1]
            return self.data_dict[category].dic[key]
        elif len(idx) == 1:
            category = idx[0]
            return self.data_dict[category]
    
    def __repr__(self) -> str:
        res =  ''
        for key, value in self.data_dict.items():
            res += f'{key}: {value.__repr__()}\n'
        return res
    
    def __str__(self) -> str:
        return self.__repr__()
        
if __name__ == "__main__":
    a = pd.DataFrame(np.random.rand(100, 5), index=pd.date_range('20210101', periods=100),
        columns=['a', 'b', 'c', 'd', 'e'])
    b = pd.DataFrame(np.random.rand(500, 5), index=pd.MultiIndex.from_product(
        [pd.date_range('20210101', periods=100), list('abcde')]),
        columns=['id1', 'id2', 'id3', 'id4', 'id5'])
    c = pd.Series(np.random.rand(500), index=pd.MultiIndex.from_product(
        [pd.date_range('20210101', periods=100), list('abcde')]), name='id6')
    factor = Data('factor', b, c, id7=a)
    forward = Data('forward', m1=a)
    price = Data('price', close=c, high=a)
    group = Data('group', c)
    collection = DataCollection(factor, forward, price, group)
    print(collection)
