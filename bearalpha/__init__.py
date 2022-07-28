"""BearAlpha
============

Bearalpha is a quantative research toolkit, but more of that

Quantative analysis
--------------------

The package provide a lot of dataframe and series accessors
to facilitate the process of quantative research. The only 
thing you need to do is import the package, and create a
dataframe, the access to the accessors to analyse itself.

Examples:

>>> import bearalpha as ba
>>> data = ba.DataFrame(ba.random.rand(100, 2), 
>>>     index=ba.date_range('20220101', periods=100, freq=ba.CBD),
>>>     columns=['a', 'b'])
>>> data.preprocessors.standarize('zscore')

Crawl the web
---------------

There are a lot of crawlers prepared in this package, like EastMoney
crawler, Guba crawler, StockUS crawler ... You can use them simply by
accessing to the classes.

Examples:

>>> import bearalpha as ba
>>> ba.StockUS.report_list()

Database construct
------------------

With the datasource from the web, or if you get a lot of data store in your
local computer, you can construct a database simply by one line of code. The
function will automaticall read the tables from your database. If you didn't
have a table for the given name, the function will create one. If you have
a table for the given name, but columns didn't match, the function will 
automatically detect that and will update the existing column while add the
new column.

Examples:

>>> import bearalpha as ba
>>> data = ba.AkShare.market_daily('000001.SZ')
>>> data.printer.display(title='000001.SZ')
>>> data.sqliter.to_sql(table='test', database='database_connection_string')
"""


from pandas import (
    DataFrame as PdDataFrame,
    Series as PdSeries,
    DateOffset,
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
    log, log10, exp,
    nan, inf,
    array, arange,
    zeros, zeros_like, ones, ones_like, empty_like, empty, eye,
    intersect1d,  
    vstack, hstack, vsplit, hsplit,
    apply_over_axes, apply_along_axis,
)

from matplotlib.pyplot import (
    subplots, 
    show, imsave, imshow,
    savefig, 
)

from pandas.tseries.offsets import (
    MonthEnd
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

__version__ = '0.1.5'


class DataFrame(PdDataFrame):
    drawer: Drawer
    printer: Printer
    regressor: Regressor
    describer: Describer
    decompositer: Decompositer
    tester: SigTester
    filer: Filer
    sqliter: Sqliter
    mysqler: Mysqler
    calculator: Calculator
    converter: Converter
    preprocessor: PreProcessor
    backtrader: BackTrader
    relocator: Relocator
    factester: Factester
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
    tester: SigTester
    filer: Filer
    sqliter: Sqliter
    mysqler: Mysqler
    calculator: Calculator
    converter: Converter
    preprocessor: PreProcessor
    backtrader: BackTrader
    relocator: Relocator
    factester: Factester
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
