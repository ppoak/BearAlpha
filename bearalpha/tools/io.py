"""Input and output functions and prettifers in this module
============================================================

You can get access to this module to prettify your output backend
by rich, or you can just register some fonts just by matplotlib

Examples:
----------

>>> import bearalpha as ba
>>> ba.reg_font('/some/path/to/your/font', 'the_name_you_give')
"""


import rich
import numpy
import pandas
import matplotlib
import backtrader
from rich.console import Console as RichConsole
from rich.progress import track
from rich.progress import Progress as RichProgress
from rich.traceback import install
from rich.table import Table
from rich.progress import (
    BarColumn,
    TimeRemainingColumn,
    MofNCompleteColumn,
    SpinnerColumn,
)


CONSOLE = RichConsole()
PROGRESS = RichProgress(
    SpinnerColumn(spinner_name='monkey'), 
    BarColumn(), 
    MofNCompleteColumn(), 
    TimeRemainingColumn())
install(suppress=[rich, pandas, numpy, matplotlib, backtrader], console=CONSOLE)


def reg_font(fontpath: str, fontname: str):
    """Register a font in matplotlib and use it

    fontpath: str, the path of the font
    fontname: str, the name of the font    
    """
    
    from matplotlib import font_manager
    import matplotlib.pyplot as plt
    font_manager.fontManager.addfont(fontpath)
    plt.rcParams['font.sans-serif'] = fontname
