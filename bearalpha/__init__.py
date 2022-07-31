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

from .core import (
    Request,
    ProxyRequest,
    Cache,
    chinese_trading_days,
    Strategy,
    Indicator,
    Analyzer,
    Observer,
    OrderTable,
)

from .tools import (
    CONSOLE,
    Table,
    latest_report_period,
    strip_stock_code, 
    wrap_stock_code,
    timeit,
    track,
    progressor,
    beautify_traceback,
    reg_font,
)


__version__ = '0.1.5'
