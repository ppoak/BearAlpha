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
from six import with_metaclass
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

class Singleton(type):
    _instance = None

    def __call__(cls, *args, **kwargs):
        if cls is not cls._instance:
            cls._instance = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instance


class Console(with_metaclass(Singleton, RichConsole)):
    ...


def progressor(
    *columns: str,
    console = None,
    auto_refresh: bool = True,
    refresh_per_second: float = 10,
    speed_estimate_period: float = 30.0,
    transient: bool = False,
    redirect_stdout: bool = True,
    redirect_stderr: bool = True,
    get_time = None,
    disable: bool = False,
    expand: bool = False,
):
    return RichProgress(
        SpinnerColumn(spinner_name='monkey'), 
        BarColumn(), 
        MofNCompleteColumn(), 
        TimeRemainingColumn(),
        *columns,
        console = console,
        auto_refresh = auto_refresh,
        refresh_per_second = refresh_per_second,
        speed_estimate_period = speed_estimate_period,
        transient = transient,
        redirect_stdout = redirect_stdout,
        redirect_stderr = redirect_stderr,
        get_time = get_time,
        disable = disable,
        expand = expand,
    )
    
def beautify_traceback(
    *,
    console = None,
    width: int = 100,
    extra_lines: int = 3,
    theme: str = None,
    word_wrap: bool = False,
    show_locals: bool = False,
    indent_guides: bool = True,
    suppress: 'str | list' = None,
    max_frames: int = 100
):
    """Enable traceback beautifier backend by rich"""
    import backtrader
    install(
        console = console,
        suppress = [rich, pandas, numpy, matplotlib, backtrader] or suppress, 
        width = width,
        extra_lines = extra_lines,
        theme = theme,
        word_wrap = word_wrap,
        show_locals = show_locals,
        indent_guides = indent_guides,
        max_frames = max_frames,
    )

def reg_font(fontpath: str, fontname: str):
    """Register a font in matplotlib and use it

    fontpath: str, the path of the font
    fontname: str, the name of the font    
    """
    
    from matplotlib import font_manager
    import matplotlib.pyplot as plt
    font_manager.fontManager.addfont(fontpath)
    plt.rcParams['font.sans-serif'] = fontname
