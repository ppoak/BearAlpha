import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
from matplotlib.widgets import Cursor
from .base import *
from ..tools import *


class ArtistError(FrameWorkError):
    pass


@pd.api.extensions.register_dataframe_accessor("drawer")
@pd.api.extensions.register_series_accessor("drawer")
class Drawer(Worker):
    """Drawer is a staff of quool for visulaizing data"""

    def draw(
        self, 
        kind: str, 
        datetime: str = slice(None), 
        asset: str = slice(None), 
        indicator: str = slice(None), 
        **kwargs
    ):
        """Draw a image of the given slice of data
        ------------------------------------------

        kind: str, the kind of the plot
        datetime: str, the slice of datetime, default to all time period
        asset: str, the slice of asset, default to all assets
        indicator: str, the slice of indicator, default to all indicators
        kwargs: dict, the kwargs for the plot function
        """
        plotwised = self._flat(datetime, asset, indicator)
        
        if not isinstance(plotwised, (pd.Series, pd.DataFrame)):
            raise ArtistError('draw', 'Your slice data seems not to be a plotable data')
        
        ax = kwargs.pop('ax', None)
        if ax is None:
            _, ax = plt.subplots(1, 1, figsize=(12, 8))

        # bar plot
        if kind == 'bar' and Worker.ists(plotwised):
            plotwised.plot.bar(ax=ax, **kwargs)
            ax.set_xticks(plotwised.index[::20])
            ax.set_xticklabels(plotwised.index.strftim('%b\n%Y')[::20])
        
        # candle plot
        elif Worker.isframe(plotwised) and Worker.ists(plotwised) and kind == "candle" and \
            pd.Index(['open', 'high', 'low', 'close']).isin(plotwised.columns).all():
            mpf.plot(plotwised, ax=ax, style='charles')
                    
        else:
            plotwised.plot(kind=kind, ax=ax, **kwargs)

        Cursor(ax, useblit=False, color='grey', lw=0.5, horizOn=True, vertOn=True, ls=':')
        return ax


@pd.api.extensions.register_dataframe_accessor("printer")
@pd.api.extensions.register_series_accessor("printer")
class Printer(Worker):
    
    def display(
        self, 
        title: str = "Table",
        datetime: str = slice(None), 
        asset: str = slice(None),
        indicator: str = slice(None), 
        maxdisplay_length: int = 20, 
        maxdisplay_width: int = 12, 
    ):
        """Print the dataframe or series in a terminal
        ------------------------------------------

        datetime: str, the slice on the datetime dimension
        asset: str, the slice on the asset dimension
        indicator: str, the slice on the indicator dimension
        maxdisplay_length: int, the maximum display length of the table
        maxdisplay_width: int, the maximum display width of the table
        title: str, the title of the table
        """
        printwised = self._flat(datetime, asset, indicator)

        printwised = printwised.reset_index().astype('str')
        
        # the table is too long, the first and last can be printed
        if printwised.shape[0] > maxdisplay_length:
            printwised = printwised.iloc[list(range(maxdisplay_length // 2 + 1))
                + list(range(-maxdisplay_length // 2, 0))]
            printwised.iloc[maxdisplay_length // 2] = '...'
        # the table is too wide, the first and last can be printed
        if printwised.shape[1] > maxdisplay_width:
            printwised = printwised.iloc[:, [i for i in range(maxdisplay_width // 2 + 1)] 
                + [i for i in range(-maxdisplay_width // 2, 0)]]
            printwised.iloc[:, maxdisplay_width // 2] = '...'
        
        table = Table(title=title)
        for col in printwised.columns:
            table.add_column(str(col), justify="center", no_wrap=True)
        for row in printwised.index:
            table.add_row(*printwised.loc[row].tolist())

        Console().print(table)

if __name__ == "__main__":
    pass