from pandas import (
    DataFrame as PdDataFrame,
    Series as PdSeries,
    Timestamp,
    MultiIndex,
    Index,
    DatetimeIndex,
    date_range, to_datetime,
    concat, merge,
    cut, qcut,
    get_dummies, factorize,
    pivot, pivot_table,
    read_excel, read_parquet, read_csv,
    )

from numpy import (
    random,
    intersect1d,
    log, log10, exp,
    nan, inf,
)

from matplotlib.pyplot import (
    subplots,
)

from backtrader import (
    indicators, talib,
    And, Or, Sum,
    Order,
)


from .tools import *
from .core import *
from .database import *
from .quool import *
from .crawl import *

__version__ = '0.1.4'


class DataFrame(PdDataFrame):
    drawer: Drawer
    printer: Printer
    regressor: Regressor
    describer: Describer
    decompositer: Decompositer
    tester: Tester
    filer: Filer
    sqliter: Sqliter
    mysqler: Mysqler
    calculator: Calculator
    converter: Converter
    preprocessor: PreProcessor
    backtrader: BackTrader
    relocator: Relocator
    evaluator: Evaluator

    def __init__(
        self,
        data = None,
        index = None,
        columns = None,
        dtype = None,
        copy = None,
    ) -> 'DataFrame':
        super().__init__(
            data=data, 
            index=index,
            columns=columns,
            dtype=dtype,
            copy=copy,
        )


class Series(PdSeries):
    drawer: Drawer
    printer: Printer
    regressor: Regressor
    describer: Describer
    decompositer: Decompositer
    tester: Tester
    filer: Filer
    sqliter: Sqliter
    mysqler: Mysqler
    calculator: Calculator
    converter: Converter
    preprocessor: PreProcessor
    backtrader: BackTrader
    relocator: Relocator
    evaluator: Evaluator

    def __init__(
        self,
        data = None,
        index = None,
        dtype = None,
        name = None,
        copy = False,
        fastpath = False,
    ) -> 'Series':
        super().__init__(
            data = data,
            index = index,
            dtype = dtype,
            name = name,
            copy = copy,
            fastpath = fastpath,
        )
