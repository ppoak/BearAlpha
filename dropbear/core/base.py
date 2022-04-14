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
            return match.group(1) + match.group(2)
        else:
            return key

    @property
    def dic(self) -> 'dict':
        return self.data_dict
    
    @property
    def dfi(self) -> 'pd.DataFrame':
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
    def dfc(self) -> 'pd.DataFrame':
        if not self.data_dict:
            return pd.DataFrame()
        col0 = []
        col1 = []
        for key in self.data_dict.keys():
            col0 += [key] * len(self.data_dict[key].columns)
            col1 += self.data_dict[key].columns.to_list()
        col = pd.MultiIndex.from_arrays([col0, col1], names=["indicator", "asset"])
        df = pd.concat(self.data_dict.values(), axis=1)
        df.columns = col
        return df
    
    @property
    def names(self) -> 'list':
        return list(self.dic.keys())
    
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
    def __init__(self, 
        factor: 'Data', 
        forward: 'Data' = None, 
        price: 'Data' = None, 
        group: 'Data' = None,
        infer_forward: 'str | list' = None
        ) -> 'None':
        '''DataCollection is used for getting ready for subsequent analysis
        --------------------------------------------------------------------

        factor: Data, the factor data
        forward: Data or None, the forward data
        price: Data or None, the price data
        group: Data or None, the group data
        infer_forward: str or list, the forward data will be inferred from
        '''
        if isinstance(infer_forward, str):
            infer_forward = [infer_forward]

        if infer_forward is not None and price is not None:
            if forward is not None:
                print(f'[!] forward is not None, infer_forward {infer_forward} cover forward')
            forward = self.__infer_forward_from_price(price, infer_forward)

        self.factor = factor
        self.forward = forward
        self.price = price
        self.group = group
        self.data_dict = {
            "factor": factor,
            "forward": forward,
            "price": price,
            "group": group
        }

    def __infer_forward_from_price(self, price: 'Data', infer_forward: 'list') -> 'pd.Series':
        '''infer forward return from ohlc price data'''
        def _infer(start_name: 'str', end_name: 'str') -> 'dict':
            forward = {}
            for iff in infer_forward:
                price_start = price[start_name].resample(iff).first()
                price_end = price[end_name].resample(iff).last()
                forward[iff] = (price_end - price_start) / price_start
            return forward

        price = price.copy()
        has_close = price.get('close', False)
        has_open = price.get('open', False)
        if len(price) == 1:
            forward = _infer(price.names[0], price.names[0])
        elif not has_close and not has_open:
            raise ValueError('Ambiguous infer forward, Please calculate manually')
        elif not has_close and has_open:
            print('[!] Not find close price, infer forward from open price')
            forward = _infer('open', 'open')
        elif has_close and not has_open:
            print('[!] Not find open price, use close price to infer forward')
            forward = _infer('close', 'close')
        else:
            forward = _infer('open', 'close')

        return Data(name="forward", **forward)
    
    def __getitem__(self, idx: 'tuple') -> 'pd.DataFrame | Data':
        if len(idx) == 2:
            category = idx[0]
            key = idx[1]
            return self.data_dict[category].dic[key]
        elif len(idx) == 1:
            category = idx[0]
            return self.data_dict[category]
    
    def __repr__(self) -> str:
        contain_forward = True if self.forward is not None else False
        contain_price = True if self.price is not None else False
        contain_group = True if self.group is not None else False
        return f'Data(factor= {self.factor}, ' \
            f'forward={contain_forward}, ' \
            f'group={contain_group}, price={contain_price})\n' \
            f'Forward: {self.forward}\n' \
            f'Group: {self.group}\nPrice: {self.price}'
    
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
    group = Data('group', b)
    collection = DataCollection(factor, forward, price, group, infer_forward='10d')
    print(collection)
