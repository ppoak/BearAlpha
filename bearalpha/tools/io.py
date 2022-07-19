import rich
import numpy
import pandas
import matplotlib
import backtrader
from rich.console import Console as RichConsole
from rich.progress import track as Track
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
