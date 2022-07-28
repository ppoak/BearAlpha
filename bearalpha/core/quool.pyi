import datetime
import backtrader as bt
import numpy as np
from bearalpha import *


class FrameWorkError(Exception):
    def __init__(self, func: str, hint: str) -> None:...
    def __str__(self) -> str: ...


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

    @staticmethod
    def series2frame(data: Series, name: str = None) -> DataFrame: ...
    @staticmethod
    def frame2series(data: DataFrame, name: str = None) -> Series: ...
    @staticmethod
    def ists(data: 'DataFrame | Series') -> bool: ...
    @staticmethod
    def iscs(data: 'DataFrame | Series') -> bool: ...
    @staticmethod
    def ispanel(data: 'DataFrame | Series') -> bool: ...
    @staticmethod
    def isframe(data: 'DataFrame | Series') -> bool: ...
    @staticmethod
    def isseries(data: 'DataFrame | Series') -> bool: ...
    @staticmethod
    def ismi(index: Index) -> bool: ...
    def _validate(self) -> None: ...
    def _flat(self, datetime, asset, indicator) -> DataFrame: ...
    def _to_array(self, *axes) -> array: ...

class Strategy(bt.Strategy):
    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO') -> None: ...
    def notify_order(self, order: bt.Order) -> None: ...
    def notify_trade(self, trade) -> None: ...

class Indicator(bt.Indicator):
    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO') -> None: ...

class Analyzer(bt.Analyzer):
    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO') -> None: ...

class Observer(bt.Observer):
    def log(self, text: str, datetime: datetime.datetime = None, hint: str = 'INFO') -> None: ...

class OrderTable(Analyzer):
    def notify_order(self, order) -> None: ...
    def get_analysis(self) -> DataFrame: ...

def async_job(
    jobs: list, 
    func: ..., 
    args: tuple = (), 
    processors: int = 4, 
    callback: ... = None, 
    kwargs: dict = {}
) -> None: ...

def from_array(
    arr: np.ndarray,
    index: Index = None, 
    columns: Index = None, 
    index_axis: 'int | list | tuple' = None,
    columns_axis: 'int | list | tuple' = None,
) -> 'DataFrame | Series': 
    """Create a DataFrame from multi-dimensional array
    ---------------------------------------------------

    arr: np.ndarray, a multi-dimensional array
    index: pd.Index, the index used as in row
    columns: pd.Index, the index used as in column
    index_axis: int, list or tuple, the sequence of axes used to transpose from original to result
    columns_axis, int, list or tuple, the sequence of axes used to transpose from original to result 
    """