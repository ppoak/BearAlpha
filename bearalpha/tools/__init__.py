"""BearAlpha tool classes, functions and constants
==================================================

All variables here are useful to the higher level of the package,
including time processing function, output function and so on.

Also, if you are outside of the package, you can still get access
to these functions.

Examples:
----------------

>>> import bearalpha as ba
>>> ba.str2time('2020-01-01')
"""


from .common import (
    time2str,
    str2time,
    item2list,
    hump2snake,
    latest_report_period,
    strip_stock_code, wrap_stock_code,
    timeit,
    MICROSECOND,
    SECOND,
    MINUTE,
    HOUR,
    DAY,
    WEEK,
    MONTH,
    YEAR,
)

from .io import (
    CONSOLE,
    PROGRESS,
    Table,
    track,
    reg_font,
)
