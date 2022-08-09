import os
import re
import datetime
import numpy as np
import pandas as pd
import backtrader as bt
from ..tools import *


class FrameWorkError(Exception):
    def __init__(self, func: str, hint: str) -> None:
        self.func = func
        self.hint = hint
    
    def __str__(self) -> str:
        return f'[-] <{self.func}> {self.hint}'


class Worker(object):
    TSFR = 1
    CSFR = 2
    PNFR = 3
    TSSR = 4
    CSSR = 5
    PNSR = 6
    MISR = 8
    MIFR = 9
    MCFR = 10
    MIMC = 11
    
    def __init__(self, data: 'pd.DataFrame | pd.Series'):
        self.data = data
        self._validate()
    
    @staticmethod
    def series2frame(data: pd.Series, name: str = None):
        return data.to_frame(name=name or 'frame')
    
    @staticmethod
    def frame2series(data: pd.DataFrame, name: str = None):
        name = name or data.columns[0]
        data = data.iloc[:, 0].copy()
        data.name = name
        return data
    
    @staticmethod
    def ists(data: 'pd.DataFrame | pd.Series'):
        return not isinstance(data.index, pd.MultiIndex) and isinstance(data.index, pd.DatetimeIndex)
    
    @staticmethod
    def iscs(data: 'pd.DataFrame | pd.Series'):
        if isinstance(data, pd.DataFrame):
            return not isinstance(data.index, pd.MultiIndex) and not isinstance(data.index, pd.DatetimeIndex) \
                and not isinstance(data.columns, pd.MultiIndex)
        else:
            return not isinstance(data.index, pd.MultiIndex) and not isinstance(data.index, pd.DatetimeIndex)
    
    @staticmethod
    def ispanel(data: 'pd.DataFrame | pd.Series'):
        return isinstance(data.index, pd.MultiIndex) and len(data.index.levshape) >= 2 \
                and isinstance(data.index.levels[0], pd.DatetimeIndex)
    
    @staticmethod
    def isframe(data: 'pd.DataFrame | pd.Series'):
        return isinstance(data, pd.DataFrame)
    
    @staticmethod
    def isseries(data: 'pd.DataFrame | pd.Series'):
        return isinstance(data, pd.Series)

    @staticmethod
    def ismi(index: pd.Index):
        return isinstance(index, pd.MultiIndex)

    def _validate(self):

        is_frame = self.isframe(self.data)
        is_series = self.isseries(self.data)
        
        if is_frame and self.data.columns.size == 1:
            is_frame = False
            is_series = True
            self.data = self.frame2series(self.data)
            
        if self.data.empty:
            print('[!] Dataframe or Series is empty')

        is_ts = self.ists(self.data)
        is_cs = self.iscs(self.data)
        is_panel = self.ispanel(self.data)

        is_mc = False if is_series else self.ismi(self.data.columns)
        is_mi = self.ismi(self.data.index)
        
        if is_ts and is_frame:
            self.type_ = Worker.TSFR
        elif is_cs and is_frame:
            self.type_ = Worker.CSFR
        elif is_panel and is_frame:
            self.type_ = Worker.PNFR
        elif is_ts and is_series:
            self.type_ = Worker.TSSR
        elif is_cs and is_series:
            self.type_ = Worker.CSSR
        elif is_panel and is_series:
            self.type_ = Worker.PNSR
        elif is_mi and is_series:
            self.type_ = Worker.MISR
        elif is_mi and is_frame and not is_mc:
            self.type_ = Worker.MIFR
        elif not is_mi and is_frame and is_mc:
            self.type_ = Worker.MCFR
        elif is_mi and is_frame and is_mc:
            self.type_ = Worker.MIMC
        else:
            raise FrameWorkError('_validate', 'Your data seems not supported in our framework')
 
    def _flat(self, datetime, asset, indicator):
        
        data = self.data.copy()
        check = (not isinstance(datetime, slice), not isinstance(asset, slice), not isinstance(indicator, slice))

        if self.type_ == Worker.PNFR:
            # is a panel and is a dataframe
            if check == (False, False, False):
                raise ValueError('Must assign at least one of dimension')
            elif check == (False, True, True):
                return data.loc[(datetime, asset), indicator].droplevel(1)
            elif check == (True, False, True):
                return data.loc[(datetime, asset), indicator].droplevel(0)
            elif check == (True, True, False):
                return data.loc[(datetime, asset), indicator]
            elif check == (True, False, False):
                return data.loc[(datetime, asset), indicator].droplevel(0)
            elif check == (False, True, False):
                return data.loc[(datetime, asset), indicator].droplevel(1)
            elif check == (False, False, True):
                return data.loc[(datetime, asset), indicator].unstack(level=1)
            elif check == (True, True, True):
                print('[!] single value was selected')
                return data.loc[(datetime, asset), indicator]
                
        elif self.type_ == Worker.PNSR:
            # is a panel and is a series
            if (check[-1] or not any(check)):
                if check[-1]:
                    print("[!] Your data is not a dataframe, indicator will be ignored")
                return data.unstack()
            elif check[1]:
                return data.loc[(datetime, asset)].unstack()
            elif check[0]:
                return data.loc[(datetime, asset)]
                
        # not a panel and is a series
        elif self.type_ == Worker.TSSR:
            return data.loc[datetime]
        elif self.type_ == Worker.CSSR:
            return data.loc[asset]
        # not a panel and is a dataframe
        elif self.type_ == Worker.CSFR:
            return data.loc[(asset, indicator)]
        elif self.type_ == Worker.TSFR:
            return data.loc[(datetime, indicator)]
        
        else:
            return data.copy()

    def _to_array(self, *axes):

        values = self.data.values.copy()
        if self.type_ == Worker.PNFR or self.type_ == Worker.PNSR:
            if not self.ismi(self.data.columns):
                revalues = values.reshape([self.data.index.levels[i].size 
                    for i in range(len(self.data.index.levels))] + [self.data.columns.size])
            else:
                revalues = values.reshape([self.data.index.levels[i].size
                    for i in range(len(self.data.index.levels))] + [self.data.columns.levels[i].size
                    for i in range(len(self.data.columns.levels))])
        elif self.type_ == Worker.TSFR or self.type_ == Worker.TSSR \
            or self.type_ == Worker.CSFR or self.type_ == Worker.CSSR:
            revalues = values
        elif self.type_ == Worker.MIMC:
            revalues = values.reshape([self.data.index.levels[i].size
                for i in range(len(self.data.index.levels))] + [self.data.columns.levels[i].size
                for i in range(len(self.data.columns.levels))])
        elif self.type_ == Worker.OTMI:
            revalues = values.reshape([self.data.index.levels[i].size 
                for i in range(len(self.data.index.levels))] + [self.data.columns.size])
        elif self.type_ == Worker.OTMC:
            revalues = values.reshape([self.data.index.size] + [self.data.columns.levels[i].size
                for i in range(len(self.data.columns.levels))])

        if axes:
            return revalues.transpose(*axes)
        else:
            return revalues


class Strategy(bt.Strategy):

    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO'):
        """Logging function"""
        datetime = datetime or self.data.datetime.date(0)
        datetime = time2str(datetime)
        if hint == "INFO":
            color = "color"
        elif hint == "WARN":
            color = "yellow"
        elif hint == "ERROR":
            color = "red"
        else:
            color = "blue"
        Console().print(f'[{color}][{hint}][/{color}] {datetime}: {text}')

    def notify_order(self, order: bt.Order):
        """order notification"""
        # order possible status:
        # 'Created'、'Submitted'、'Accepted'、'Partial'、'Completed'、
        # 'Canceled'、'Expired'、'Margin'、'Rejected'
        # broker submitted or accepted order do nothing
        if order.status in [order.Submitted, order.Accepted, order.Created]:
            return

        # broker completed order, just hint
        elif order.status in [order.Completed]:
            self.log(f'Trade <{order.executed.size}> <{order.info.get("name", "data")}> at <{order.executed.price:.2f}>')
            # record current bar number
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected, order.Expired]:
            self.log('Order canceled, margin, rejected or expired', hint='WARN')

        # except the submitted, accepted, and created status,
        # other order status should reset order variable
        self.order = None

    def notify_trade(self, trade):
        """trade notification"""
        if not trade.isclosed:
            # trade not closed, skip
            return
        # else, log it
        self.log(f'Gross Profit: {trade.pnl:.2f}, Net Profit {trade.pnlcomm:.2f}')


class Indicator(bt.Indicator):
    
    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO'):
        """Logging function"""
        datetime = datetime or self.data.datetime.date(0)
        datetime = time2str(datetime)
        if hint == "INFO":
            color = "color"
        elif hint == "WARN":
            color = "yellow"
        elif hint == "ERROR":
            color = "red"
        else:
            color = "blue"
        Console().print(f'[{color}][{hint}][/{color}] {datetime}: {text}')
    

class Analyzer(bt.Analyzer):

    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO'):
        """Logging function"""
        datetime = datetime or self.data.datetime.date(0)
        datetime = time2str(datetime)
        if hint == "INFO":
            color = "color"
        elif hint == "WARN":
            color = "yellow"
        elif hint == "ERROR":
            color = "red"
        else:
            color = "blue"
        Console().print(f'[{color}][{hint}][/{color}] {datetime}: {text}')


class Observer(bt.Observer):

    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO'):
        """Logging function"""
        datetime = datetime or self.data.datetime.date(0)
        datetime = time2str(datetime)
        if hint == "INFO":
            color = "color"
        elif hint == "WARN":
            color = "yellow"
        elif hint == "ERROR":
            color = "red"
        else:
            color = "blue"
        Console().print(f'[{color}][{hint}][/{color}] {datetime}: {text}')


class OrderTable(Analyzer):

    def __init__(self):
        self.orders = []

    def notify_order(self, order):
        if order.status == order.Completed:
            if order.isbuy():
                self.orders.append([
                    self.data.datetime.date(0),
                    order.info.get('name', 'data'), order.executed.size, 
                    order.executed.price, 'BUY']
                )
            elif order.issell():
                self.orders.append([
                    self.data.datetime.date(0),
                    order.info.get('name', 'data'), order.executed.size, 
                    order.executed.price, 'SELL']
                )
        
    def get_analysis(self):
        self.rets = pd.DataFrame(self.orders, columns=['datetime', 'asset', 'size', 'price', 'direction'])
        self.rets = self.rets.set_index('datetime')
        return self.orders


def from_array(
    arr: np.ndarray,
    index: pd.Index = None, 
    columns: pd.Index = None, 
    index_axis: 'int | list | tuple' = None,
    columns_axis: 'int | list | tuple' = None,
):
    """Create a DataFrame from multi-dimensional array
    ---------------------------------------------------

    arr: np.ndarray, a multi-dimensional array
    index: pd.Index, the index used as in row
    columns: pd.Index, the index used as in column
    index_axis: int, list or tuple, the sequence of axes used to transpose from original to result
    columns_axis, int, list or tuple, the sequence of axes used to transpose from original to result 
    """
    if index_axis is None and columns_axis is None:
        index_axis = [i for i in range(len(arr.shape) - 1)]
        columns_axis = [len(arr.shape) - 1]
    elif index_axis is None and columns_axis is not None:
        columns_axis = list(columns_axis) if isinstance(columns_axis, tuple) else item2list(columns_axis)
        index_axis = list(set(range(len(arr.shape))) - set(columns_axis))
    elif index_axis is not None and columns_axis is None:
        index_axis = list(index_axis) if isinstance(index_axis, tuple) else item2list(index_axis)
        columns_axis = list(set(range(len(arr.shape))) - set(index_axis))

    index_axis = item2list(index_axis) if not isinstance(index_axis, tuple) else list(index_axis)
    columns_axis = item2list(columns_axis) if not isinstance(columns_axis, tuple) else list(columns_axis)
    sequ = index_axis + columns_axis
    
    arrt = arr.transpose(sequ)

    arrshape = np.array(arr.shape)
    values = arrt.reshape(arrshape[index_axis].prod(), arrshape[columns_axis].prod())

    if index is None:
        index = pd.MultiIndex.from_product([range(i) for i in arrshape[index_axis]], 
            names=[f'levels{i}' for i in range(len(index_axis))]) if len(index_axis) > 1 \
            else pd.RangeIndex(0, arrshape[index_axis].prod())
    
    if columns is None:
        columns = pd.MultiIndex.from_product([range(i) for i in arrshape[columns_axis]], 
            names=[f'levels{i}' for i in range(len(columns_axis))]) if len(columns_axis) > 1 \
            else pd.RangeIndex(0, arrshape[columns_axis].prod())

    return pd.DataFrame(data=values, index=index, columns=columns)

def concat(objs, colnames: list = None, **kwargs):
    if colnames is not None:
        concated = pd.concat(objs, **kwargs)
        concated.columns = colnames
    else:
        concated = pd.concat(objs, **kwargs)
    return concated

def read_csv(
    path_or_buffer,
    perspective: str = None,
    name_pattern: str = None,      
    **kwargs
):
    '''A enhanced function for reading files in a directory to a panel DataFrame
    ----------------------------------------------------------------------------

    path: path to the directory
    perspective: 'datetime', 'asset', 'indicator'
    name_pattern: pattern to match the file name, which will be extracted as index
    kwargs: other arguments for pd.read_csv

    **note: the name of the file in the directory will be interpreted as the 
    sign(column or index) to the data, so set it to the brief one
    '''

    if not os.path.isdir(path_or_buffer):
        return pd.read_csv(path_or_buffer, **kwargs)
    
    files = sorted(os.listdir(path_or_buffer))
    datas = []

    if perspective is None:
        for file in files:
            data = pd.read_csv(os.path.join(path_or_buffer, file), **kwargs) 
            datas.append(data)
        datas = pd.concat(datas, axis=0).sort_index()
    
    elif perspective == "indicator":
        name_pattern = name_pattern or r'.*'
        for file in files:
            basename = os.path.splitext(file)[0]
            name = re.findall(name_pattern, basename)[0]
            data = pd.read_csv(os.path.join(path_or_buffer, file), **kwargs)
            data = data.stack()
            data.name = name
            datas.append(data)
        datas = pd.concat(datas, axis=1).sort_index()

    elif perspective == "asset":
        name_pattern = name_pattern or r'[a-zA-Z\d]{6}\.[a-zA-Z]{2}|[a-zA-Z]{0,2}\..{6}'
        for file in files:
            basename = os.path.splitext(file)[0]
            name = re.findall(name_pattern, basename)[0]
            data = pd.read_csv(os.path.join(path_or_buffer, file), **kwargs)
            data.index = pd.MultiIndex.from_product([data.index, [name]])
            datas.append(data)
        datas = pd.concat(datas).sort_index()
        
    elif perspective == "datetime":
        name_pattern = name_pattern or r'\d{4}[./-]\d{2}[./-]\d{2}|\d{4}[./-]\d{2}[./-]\d{2}\s?\d{2}[:.]\d{2}[:.]\d{2}'
        for file in files:
            basename = os.path.splitext(file)[0]
            name = re.findall(name_pattern, basename)[0]
            data = pd.read_csv(os.path.join(path_or_buffer, file), **kwargs)
            data.index = pd.MultiIndex.from_product([pd.to_datetime([name]), data.index])
            datas.append(data)
        datas = pd.concat(datas).sort_index()
    
    return datas

def read_excel(
    path_or_buffer,
    perspective: str = None,
    name_pattern: str = None,
    **kwargs,
):
    '''A enhanced function for reading files in a directory to a panel DataFrame
    ----------------------------------------------------------------------------

    path: path to the directory
    perspective: 'datetime', 'asset', 'indicator'
    kwargs: other arguments for pd.read_excel

    **note: the name of the file in the directory will be interpreted as the 
    sign(column or index) to the data, so set it to the brief one
    '''

    if not os.path.isdir(path_or_buffer):

        if perspective is None:
            return pd.read_excel(path_or_buffer, **kwargs)
        
        sheets_dict = pd.read_excel(path_or_buffer, sheet_name=None, **kwargs)
        datas = []

        if perspective == "indicator":
            for indicator, data in sheets_dict.items():
                data = data.stack()
                data.name = indicator
                datas.append(data)
            datas = pd.concat(datas, axis=1)

        elif perspective == "asset":
            for asset, data in sheets_dict.items():
                data.index = pd.MultiIndex.from_product([data.index, [asset]])
                datas.append(data)
            datas = pd.concat(datas)
            datas = data.sort_index()

        elif perspective == "datetime":
            for datetime, data in sheets_dict.items():
                data.index = pd.MultiIndex.from_product([[datetime], data.index])
                datas.append(data)
            datas = pd.concat(datas)

        else:
            raise ValueError('perspective must be in one of datetime, indicator or asset')
        
        return datas

    else:

        files = os.listdir(path_or_buffer)
        datas = []
        
        if perspective is None:
            for file in files:
                data = pd.read_csv(os.path.join(path_or_buffer, file), **kwargs) 
                datas.append(data)
            datas = pd.concat(datas, axis=0).sort_index()

        elif perspective == "indicator":
            name_pattern = name_pattern or r'.*'
            for file in files:
                basename = os.path.splitext(file)[0]
                name = re.findall(name_pattern, basename)[0]
                data = pd.read_excel(os.path.join(path_or_buffer, file), **kwargs)
                data = data.stack()
                data.name = name
                datas.append(data)
            datas = pd.concat(datas, axis=1).sort_index()

        elif perspective == "asset":
            name_pattern = name_pattern or r'[a-zA-Z\d]{6}\.[a-zA-Z]{2}|[a-zA-Z]{0,2}\..{6}'
            for file in files:
                basename = os.path.splitext(file)[0]
                name = re.search(name_pattern, basename).group()
                data = pd.read_excel(os.path.join(path_or_buffer, file), **kwargs)
                data.index = pd.MultiIndex.from_product([data.index, [name]])
                datas.append(data)
            datas = pd.concat(datas).sort_index()
            
        elif perspective == "datetime":
            name_pattern = name_pattern or r'\d{4}[./-]\d{2}[./-]\d{2}|\d{4}[./-]\d{2}[./-]\d{2}\s?\d{2}[:.]\d{2}[:.]\d{2}'
            for file in files:
                basename = os.path.splitext(file)[0]
                name = re.search(name_pattern, basename).group()
                data = pd.read_excel(os.path.join(path_or_buffer, file), **kwargs)
                data.index = pd.MultiIndex.from_product([pd.to_datetime([name]), data.index])
                datas.append(data)
            datas = pd.concat(datas).sort_index()
        
        return datas
